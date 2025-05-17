import base64
import json
import os
import subprocess
import time

import yaml
from kubernetes import client, config


class GardenerHelper:
    def __init__(self, kubeconfig_path, namespace, shoot_name):
        """
        Initialize the GardenerHelper object.

        :param kubeconfig_path: Path to the kubeconfig file.
        :param namespace: The namespace where the shoot resource resides.
        :param shoot_name: The name of the shoot resource.
        """
        self.kubeconfig_path = kubeconfig_path
        self.namespace = namespace
        self.shoot_name = shoot_name

        # Load the kubeconfig and create the CustomObjectsApi instance.
        config.load_kube_config(config_file=self.kubeconfig_path)
        self.custom_api = client.CustomObjectsApi()

    def safe_call_with_retries(self, func, max_retries=3, interval=10):
        """
        Safely call a function with retries.
        :param func: The function to call.
        :param max_retries: The maximum number of retries.
        :param interval: Time (in seconds) between each retry.
        :return: The result of the function call.
        """
        retries = 0
        while retries < max_retries:
            try:
                return func()
            except Exception as e:
                print(f"Error occurred: {e}")
                retries += 1
                time.sleep(interval)
        raise Exception("Max retries exceeded. Aborting operation.")

    def determine_cloud_provider(self):
        """
        Determine the cloud provider from the context.
        :return: The cloud provider type (aws, azure, gcp, etc.) or None if it couldn't be determined
        """
        # First, ensure the context is generated
        try:
            subprocess.run(["iac", "-d", "cluster", "context"], check=True)
            print("Successfully generated context")
        except subprocess.CalledProcessError as e:
            print(f"Warning: Failed to generate context: {e}")
            print("Will try to use existing context file if available")
        
        # Get the path to the ctx.yml file
        # Using relative path from the known locations
        script_dir = os.path.dirname(os.path.abspath(__file__))
        base_dir = os.path.normpath(os.path.join(script_dir, "../../../../../../.."))  
        
        # Try to find ctx.yml in common locations
        possible_ctx_paths = [
            os.path.join(base_dir, "deployments/cluster/gen/ctx.yml")
        ]
        
        ctx_path = None
        for path in possible_ctx_paths:
            norm_path = os.path.normpath(path)
            if os.path.exists(norm_path):
                ctx_path = norm_path
                print(f"Found context file at: {ctx_path}")
                break
        
        if not ctx_path:
            print("Could not find context file. Please ensure 'iac -d cluster context' has been run.")
            return None
        
        # Read and parse the context file
        try:
            with open(ctx_path, 'r') as f:
                ctx_data = yaml.safe_load(f)
                
            # Try to determine provider from context structure
            if 'context' in ctx_data and 'imports' in ctx_data['context'] and 'iaas_provider' in ctx_data['context']['imports']:
                provider_data = ctx_data['context']['imports']['iaas_provider']
                
                if 'landscape' in provider_data and 'type' in provider_data['landscape']:
                    provider_type = provider_data['landscape']['type']
                    print(f"Detected cloud provider from ctx.yml: {provider_type}")
                    return provider_type
        
            # If we couldn't extract the provider from the expected structure, log it
            print("Could not determine cloud provider from context structure.")
            return None
        
        except Exception as e:
            print(f"Error reading context file {ctx_path}: {e}")
            return None

    def select_shoot_template(self):
        """
        Select the appropriate shoot template based on the cloud provider.
        
        :return: Path to the selected template
        :raises: ValueError if cloud provider couldn't be determined
        :raises: FileNotFoundError if no suitable template could be found
        """
        
        # Determine the cloud provider
        provider_type = self.determine_cloud_provider()
        if not provider_type:
            raise ValueError("Cloud provider could not be determined from context. Please specify a template file directly.")
            
        # Build paths relative to the script location
        script_dir = os.path.dirname(os.path.abspath(__file__))
        base_dir = os.path.normpath(os.path.join(script_dir, "../../../.."))
        
        # Define possible locations for templates
        templates_dirs = [
            os.path.join(base_dir, "deployments/cluster/templates"),
        ]
        
        # Find the first templates directory that exists
        templates_dir = None
        for dir_path in templates_dirs:
            norm_path = os.path.normpath(dir_path)
            if os.path.exists(norm_path) and os.path.isdir(norm_path):
                templates_dir = norm_path
                print(f"Found templates directory at: {templates_dir}")
                break
                
        if not templates_dir:
            # If no templates directory found, check if there's a shoot.yml directly in the deployments directory
            deployments_dir = os.path.join(base_dir, "deployments/cluster")
            shoot_yml_path = os.path.join(deployments_dir, "shoot.yml")
            
            if os.path.exists(shoot_yml_path):
                print(f"No templates directory found, but found shoot.yml at: {shoot_yml_path}")
                return shoot_yml_path
                
            # If we still don't have a template, raise an error
            raise FileNotFoundError(f"Could not find templates directory or shoot.yml. Create the templates directory at {templates_dirs[0]} with cloud-specific templates.")
            
        # Look for a template for the detected provider
        template_path = os.path.join(templates_dir, f"shoot-{provider_type}.yml")
        
        if not os.path.exists(template_path):
            print(f"Template for provider '{provider_type}' not found at {template_path}")
            
            # Check if there's a generic shoot.yml in the templates directory
            generic_template = os.path.join(templates_dir, "shoot.yml")
            if os.path.exists(generic_template):
                print(f"Using generic shoot.yml template: {generic_template}")
                return generic_template
                
            # Try to find any template for any provider
            for provider in ["aws", "azure", "gcp"]:
                alt_template = os.path.join(templates_dir, f"shoot-{provider}.yml")
                if os.path.exists(alt_template):
                    print(f"Warning: Using template for '{provider}' instead of '{provider_type}': {alt_template}")
                    print(f"Please create a template specific to '{provider_type}' for future deployments.")
                    return alt_template
                
            # If we still don't have a template, raise an error
            raise FileNotFoundError(f"Could not find template for '{provider_type}' provider. Please create {template_path} based on your shoot.yml configuration.")

        print(f"Selected template for '{provider_type}' provider: {template_path}")
        return template_path

    def create_shoot(self, template_file_path=None, template_string=None):
        """
        Create a shoot resource from a YAML template.

        Either provide a path to a template file or a YAML string.
        If no template is provided, it will try to select the appropriate template
        based on the cloud provider.

        :param template_file_path: Path to the shoot template YAML file.
        :param template_string: A string containing the shoot template in YAML format.
        :return: The created shoot resource object.
        :raises: ValueError if neither a file path nor a string is provided.
        """
        if not template_string and not template_file_path:
            try:
                # Try to select a template
                template_file_path = self.select_shoot_template()
                print(f"Automatically selected template: {template_file_path}")
            except Exception as e:
                error_msg = f"Error selecting template: {e}"
                print(error_msg)
                raise ValueError(f"{error_msg}. Please provide a template_file_path or template_string explicitly.")
            
        if template_string:
            shoot_template = yaml.safe_load(template_string)
        elif template_file_path:
            if not os.path.exists(template_file_path):
                raise FileNotFoundError(f"Template file does not exist: {template_file_path}")
                
            with open(template_file_path, "r") as f:
                shoot_template = yaml.safe_load(f)
        else:
            raise ValueError("Either template_file_path or template_string must be provided.")

        try:
            created_shoot = self.custom_api.create_namespaced_custom_object(
                group="core.gardener.cloud",
                version="v1beta1",
                namespace=self.namespace,
                plural="shoots",
                body=shoot_template,
            )
            print(f"Shoot resource '{self.shoot_name}' creation initiated.")
            return created_shoot
        except Exception as e:
            print("Error creating shoot resource:", e)
            raise

    def get_shoot(self):
        """
        Retrieve the shoot resource.

        :return: The shoot resource object if it exists; otherwise, None.
        """
        try:
            shoot = self.custom_api.get_namespaced_custom_object(
                group="core.gardener.cloud",
                version="v1beta1",
                namespace=self.namespace,
                plural="shoots",
                name=self.shoot_name,
            )
            return shoot
        except client.exceptions.ApiException as e:
            if e.status == 404:
                print(f"Shoot resource '{self.shoot_name}' does not exist.")
                return None
            else:
                print("Error retrieving shoot resource:", e)
                raise

    def shoot_exists(self):
        """
        Check if the shoot resource exists.

        :return: True if it exists, False otherwise.
        """
        return self.get_shoot() is not None

    def poll_shoot_status(self, timeout=300, interval=10):
        """
        Poll the status of the shoot resource.

        Returns:
            - True if the shoot's lastOperation.state is 'Succeeded',
            - False if it is 'Failed',
            - "in-progress" if polling times out without a conclusive state.

        :param timeout: Total time (in seconds) to poll before giving up.
        :param interval: Time (in seconds) between each poll.
        :return: True, False, or "in-progress"
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            shoot = self.get_shoot()
            if shoot and "status" in shoot and "lastOperation" in shoot["status"]:
                state = shoot["status"]["lastOperation"].get("state", "Progressing")
                print(f"Current shoot state: {state}")
                if state.lower() == "succeeded":
                    return True
                elif state.lower() == "failed":
                    return False
            else:
                print(f"Status not available yet for shoot: {self.shoot_name}")
            time.sleep(interval)
        return "in-progress"

    def poll_shoot_deletion_status(self, timeout=300, interval=10):
        """
        Poll for the deletion of the shoot resource.

        Returns:
            - True if the shoot resource is confirmed deleted,
            - "in-progress" if deletion is still ongoing after the timeout.

        :param timeout: Total time (in seconds) to poll for deletion.
        :param interval: Time (in seconds) between each poll.
        :return: True or "in-progress"
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                # Try to retrieve the shoot; if it exists, deletion is not complete.
                self.custom_api.get_namespaced_custom_object(
                    group="core.gardener.cloud",
                    version="v1beta1",
                    namespace=self.namespace,
                    plural="shoots",
                    name=self.shoot_name,
                )
                print(f"Shoot resource '{self.shoot_name}' still exists.")
            except client.exceptions.ApiException as e:
                if e.status == 404:
                    print(
                        f"Shoot resource '{self.shoot_name}' has been deleted successfully."
                    )
                    return True
                else:
                    print("Error while polling deletion status:", e)
            time.sleep(interval)
        return "in-progress"

    def check_shoot_health(self):
        """
        Check the health of the shoot resource.

        Returns:
            The health status if available, otherwise the last operation state or a message.
        """
        shoot = self.get_shoot()
        if shoot and "status" in shoot:
            # Prefer a dedicated health field if present.
            health = shoot["status"].get("health", None)
            if health:
                print(f"Shoot health: {health}")
                return health
            # Fallback to the last operation state.
            if "lastOperation" in shoot["status"]:
                state = shoot["status"]["lastOperation"].get("state", "Unknown")
                print(f"Shoot last operation state: {state}")
                return state
            return "Status available but no health info"
        return "No status available"

    def delete_shoot(self):
        """
        Delete the shoot resource.

        First, it annotates the resource with
            confirmation.gardener.cloud/deletion=true
        (as required for deletion), then it initiates deletion in a non-blocking way.
        """
        # Annotate the shoot resource for deletion confirmation.
        patch_body = {
            "metadata": {
                "annotations": {"confirmation.gardener.cloud/deletion": "true"}
            }
        }
        try:
            self.custom_api.patch_namespaced_custom_object(
                group="core.gardener.cloud",
                version="v1beta1",
                namespace=self.namespace,
                plural="shoots",
                name=self.shoot_name,
                body=patch_body,
            )
            print(
                f"Annotation added to shoot resource '{self.shoot_name}' for deletion confirmation."
            )
        except Exception as e:
            print("Error patching shoot resource for deletion:", e)
            raise

        # Initiate deletion (asynchronously, similar to --wait=false).
        try:
            self.custom_api.delete_namespaced_custom_object(
                group="core.gardener.cloud",
                version="v1beta1",
                namespace=self.namespace,
                plural="shoots",
                name=self.shoot_name,
                body=client.V1DeleteOptions(),
            )
            print(
                f"Deletion initiated for shoot resource '{self.shoot_name}' (wait disabled)."
            )
        except Exception as e:
            print("Error deleting shoot resource:", e)
            raise

    def get_shoot_kubeconfig(self, expiration_seconds=600):
        """
        Generate and retrieve the kubeconfig for the shoot cluster.

        This method loads the existing kubeconfig (from the instance's configuration),
        sends a request to the Gardener API to create an admin kubeconfig for the shoot,
        decodes the returned kubeconfig (which is base64 encoded), and then creates a new
        API client for interacting with the shoot cluster.

        :param expiration_seconds: The lifetime of the generated kubeconfig.
        :return: A tuple (decoded_kubeconfig, shoot_api_client) where decoded_kubeconfig is a string
                 and shoot_api_client is a Kubernetes API client for the shoot cluster.
        """
        kubeconfig_request = {
            "apiVersion": "authentication.gardener.cloud/v1alpha1",
            "kind": "AdminKubeconfigRequest",
            "spec": {"expirationSeconds": expiration_seconds},
        }

        # Use the API client from the loaded kubeconfig.
        api_client = self.custom_api.api_client
        try:
            response = api_client.call_api(
                resource_path=f"/apis/core.gardener.cloud/v1beta1/namespaces/{self.namespace}/shoots/{self.shoot_name}/adminkubeconfig",
                method="POST",
                body=kubeconfig_request,
                auth_settings=["BearerToken"],
                _preload_content=False,
                _return_http_data_only=True,
            )
        except Exception as e:
            print("Error requesting shoot kubeconfig:", e)
            raise

        try:
            response_json = json.loads(response.data)
            encoded_kubeconfig = response_json["status"]["kubeconfig"]
            decoded_kubeconfig = base64.b64decode(encoded_kubeconfig).decode("utf-8")
            print("Decoded shoot kubeconfig:")
            print(decoded_kubeconfig)
            shoot_config = yaml.safe_load(decoded_kubeconfig)
            shoot_api_client = config.new_client_from_config_dict(shoot_config)
            return decoded_kubeconfig, shoot_api_client
        except Exception as e:
            print("Error decoding shoot kubeconfig:", e)
            raise


# Example usage for local testing:
if __name__ == "__main__":
    kubeconfig = "./robot-kubeconfig.yml"
    namespace = "garden-perftests"
    shoot_name = "monika-vm"
    template_file = "./shoot-template.yml"

    # Create an instance of GardenerHelper.
    gardener = GardenerHelper(kubeconfig, namespace, shoot_name)

    # Create the shoot.
    gardener.create_shoot(template_file_path=template_file)
    creation_status = gardener.poll_shoot_status(timeout=300, interval=10)
    print("Shoot creation status:", creation_status)

    # Check if the shoot exists.
    exists = gardener.shoot_exists()
    print("Does shoot exist?", exists)

    # Check the health of the shoot.
    health = gardener.check_shoot_health()
    print("Shoot health:", health)

    # Retrieve the shoot kubeconfig and create a shoot API client.
    kubeconfig_str, shoot_api_client = gardener.get_shoot_kubeconfig(
        expiration_seconds=600
    )
    print("Retrieved shoot kubeconfig:")
    print(kubeconfig_str)

    # Delete the shoot.
    gardener.delete_shoot()
    deletion_status = gardener.poll_shoot_deletion_status(timeout=300, interval=10)
    print("Shoot deletion status:", deletion_status)
