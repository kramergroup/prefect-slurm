from sshfs import SSHFileSystem

fs = SSHFileSystem(
    host="hsuper-login.hsu-hh.de",
    username="kramerd",
    client_keys=["~/.ssh/id_hsuper"],
)
fs.ls(".")
del fs
