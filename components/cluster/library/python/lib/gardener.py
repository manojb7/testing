import base64
import json
import os
import shutil
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

        # Directory containing the templates
        self.template_dir = os.path.dirname(os.path.abspath(__file__)) + "/../deployments/cluster/templates"

    def determine_cloud_provider(self):
        """
        Determine the cloud provider from the context.

        :return: The cloud provider type (aws, azure, gcp, etc.)
        """
        # Check if a provider-specific context file exists and try to detect provider
        ctx_path = os.path.dirname(os.path.abspath(__file__)) + "/../deployments/cluster/gen/ctx.yml"

        if os.path.exists(ctx_path):
            try:
                with open(ctx_path, 'r') as f:
                    ctx_data = yaml.safe_load(f)

                # Try to determine provider from context structure
                if 'context' in ctx_data and 'imports' in ctx_data['context'] and 'iaas_provider' in ctx_data['context']['imports']:
                    provider_data = ctx_data['context']['imports']['iaas_provider']

                    if 'landscape' in provider_data and 'type' in provider_data['landscape']:
                        return provider_data['landscape']['type']
            except Exception as e:
                print(f"Error reading context file: {e}")

        # Default to AWS if can't determine
        print("Could not determine cloud provider, defaulting to AWS")
        return 'aws'

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

    def select_shoot_template(self):
        """
        Select the appropriate shoot template based on the cloud provider.

        :return: Path to the selected template
        """
        # Determine the cloud provider
        provider_type = self.determine_cloud_provider()

        # Get the template paths
        templates_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "deployments", "cluster", "templates")
        template_path = os.path.join(templates_dir, f"shoot-{provider_type}.yml")

        # Check if the template exists
        if not os.path.exists(template_path):
            print(f"Template for provider '{provider_type}' not found at {template_path}")
            print(f"Using default AWS template")
            template_path = os.path.join(templates_dir, "shoot-aws.yml")

            # If even the default template doesn't exist, raise an error
            if not os.path.exists(template_path):
                raise FileNotFoundError(f"Cannot find default AWS template at {template_path}")

        # Copy the template to shoot.yml in the cluster deployment directory
        shoot_yml_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "deployments", "cluster", "shoot.yml")
        shutil.copy2(template_path, shoot_yml_path)

        print(f"Selected and copied template for '{provider_type}' provider to shoot.yml")
        return shoot_yml_path

    def create_shoot(self, template_file_path=None, template_string=None):
        """
        Create a shoot resource from a YAML template.

        Either provide a path to a template file or a YAML string.
        If no template is provided, it will automatically select the appropriate
        template based on the cloud provider.

        :param template_file_path: Path to the shoot template YAML file.
        :param template_string: A string containing the shoot template in YAML format.
        :return: The created shoot resource object.
        :raises: ValueError if neither a file path nor a string is provided.
        """
        if not template_string and not template_file_path:
            # Select the appropriate template based on cloud provider
            template_file_path = self.select_shoot_template()

        if template_string:
            shoot_template = yaml.safe_load(template_string)
        elif template_file_path:
            with open(template_file_path, "r") as f:
                shoot_template = yaml.safe_load(f)
        else:
            raise ValueError(
                "Either template_file_path or template_string must be provided."
            )

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

    gardener = GardenerHelper(kubeconfig, namespace, shoot_name)

    # This will automatically select the appropriate template based on cloud provider
    gardener.create_shoot()
    creation_status = gardener.poll_shoot_status(timeout=300, interval=10)
    print("Shoot creation status:", creation_status)

    exists = gardener.shoot_exists()
    print("Does shoot exist?", exists)

    health = gardener.check_shoot_health()
    print("Shoot health:", health)

    kubeconfig_str, shoot_api_client = gardener.get_shoot_kubeconfig(
        expiration_seconds=600
    )
    print("Retrieved shoot kubeconfig:")
    print(kubeconfig_str)

    gardener.delete_shoot()
    deletion_status = gardener.poll_shoot_deletion_status(timeout=300, interval=10)
    print("Shoot deletion status:", deletion_status)
