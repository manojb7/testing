import os

import landscape_tools
import yaml

from . import common, gardener


class Config:
    def __init__(self):
        self.ctx = landscape_tools.Context.load().context
        self.landscape_name = self.ctx.landscape.name
        self.kubeconfig = self.ctx.credentials["serviceaccount"]["kubeconfig"]
        self.kubeconfig_file_path = "/tmp/kubeconfig.monitoring.tmp"
        self.definition_dir = self.ctx.imports.meta.deployment.definition
        self.gen_dir = self.ctx.imports.meta.deployment.gen
        self.shoot_file_name = self.ctx.shoot_yaml_name
        self.gardener_namespace = self.ctx.gardener_namespace
        self.cluster_name = self.ctx.vm_name
        self.rendered_shoot_path = common.run_spiff_merge(
            self.definition_dir, self.gen_dir, self.shoot_file_name
        )

    def write_kubeconfig_file(self):
        with open(self.kubeconfig_file_path, "w") as kubeconfig_file:
            kubeconfig_file.writelines(self.kubeconfig)

    def get_gardener_helper(self):
        return gardener.GardenerHelper(
            self.kubeconfig_file_path,
            self.gardener_namespace,
            self.cluster_name,
        )

    def get_dns_domain(self):
        return yaml.safe_load(open(self.rendered_shoot_path, "r"))["spec"]["dns"][
            "domain"
        ]

    def print_config(self):
        print(f"landscape_name: {self.landscape_name}")
        print(f"definition_dir: {self.definition_dir}")
        print(f"gen_dir: {self.gen_dir}")
        print(f"shoot_file_name: {self.shoot_file_name}")
        print(f"gardener_namespace: {self.gardener_namespace}")
        print(f"cluster_name: {self.cluster_name}")
        print(f"rendered_shoot_path: {self.rendered_shoot_path}")
