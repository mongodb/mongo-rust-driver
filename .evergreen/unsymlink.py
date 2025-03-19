import os
import shutil

found = []
for entry in os.scandir():
    if not entry.is_symlink():
        print(f"Skipping {entry.name}: not a symlink")
        continue
    target = os.readlink(entry.name)
    if target != "rustup.exe":
        print(f"Skipping {entry.name}: not rustup.exe")
        continue
    print(f"Found {entry.name}")
    found.append(entry.name)

for name in found:
    print(f"Replacing {name} symlink with copy")
    os.remove(name)
    shutil.copy2("rustup.exe", name)