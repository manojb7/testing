#!/usr/bin/env python3

import yaml
from landscape_tools import subprocess_helper


def read_yaml_file(component_name, filename):
    return yaml.safe_load(
        subprocess_helper.exec_subprocess(
            command=["lscrypt", "read", "-d", component_name, filename],
            print_command=False,
            print_stdout=False,
        ).stdout
    )
    return None


def write_yaml_file(component_name, filename, yamlInput):
    data = yaml.safe_dump(yamlInput, default_style="|")
    # `bosh int -` sorts the yaml file and ensures that we don't produce
    # unnecessary changes.
    data = subprocess_helper.exec_subprocess(
        command=["bosh", "int", "-"],
        print_command=False,
        print_stdout=False,
        stdin=data,
    ).stdout
    subprocess_helper.exec_subprocess(
        command=["lscrypt", "write", "-d", component_name, filename],
        print_command=False,
        print_stdout=False,
        stdin=data,
    )


class LscryptFile:
    def __init__(self, filename):
        self.filename = filename

    def read_yaml(self):
        return read_yaml_file(filename=self.filename)

    def write_yaml(self, yamlInput):
        return write_yaml_file(filename=self.filename, yamlInput=yamlInput)
