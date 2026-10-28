import os
import shutil

src = "testdata/seed/ubuntu.iso"
piece = 262144              
chunk = 64 * 1024 * 1024 

out_dir = "splits"
split1 = os.path.join(out_dir, "split1.iso")  # first half kept
split2 = os.path.join(out_dir, "split2.iso")  # second half kept

size = os.path.getsize(src)
split = (size // 2) // piece * piece

os.makedirs(out_dir, exist_ok=True)
shutil.copyfile(src, split1)
shutil.copyfile(src, split2)

def zero(path, start, end):
    with open(path, "r+b") as f:
        f.seek(start)
        pos = start
        while pos < end:
            n = min(chunk, end - pos)
            f.write(b"\0" * n)
            pos += n

zero(split1, split, size)  # split1 keeps the first half
zero(split2, 0, split)     # split2 keeps the second half

print(f"size={size} split={split}")