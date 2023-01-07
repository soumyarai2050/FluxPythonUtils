import pexpect as px


def package_install_checker(package_name: str) -> bool:
    """
    Function to check if package is installed, returns True if installed and False otherwise.
    """
    chk_cmd = px.spawn(f"apt-cache policy {package_name}")
    # chk_cmd.logfile = sys.stdout.buffer  # Just shows output of cmd when executed, not-needed (for debug)
    chk_cmd.timeout = None
    chk_cmd.expect(px.EOF)
    cmd_output = chk_cmd.before.decode("utf-8").splitlines()
    if "N: Unable to locate package" in cmd_output[0] or "Installed: (none)" in cmd_output[1]:
        return False
    return True
