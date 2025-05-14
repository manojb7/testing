import os
import landscape_tools
import subprocess

def run_spiff_merge(definition_dir, gen_dir, file_name):
    spiff_bin = "spiff"
    context_file_path = f"{gen_dir}/ctx.yml"
    file_path = f"{definition_dir}/{file_name}"
    target_file_path = f"{gen_dir}/{file_name}"
    spiff_merge_cmd = f"{spiff_bin} merge {file_path} {context_file_path}".split()
    merged_file = landscape_tools.exec_subprocess(command, capture_stdout=True).stdout
    with open(target_file_path, "w") as shoot_file:
        shoot_file.write(merged_file)
    return target_file_path


