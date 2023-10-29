set -x

# N_CPUS=1 N_ENTDB=4 RAYON_NUM_THREADS=1 cargo run --release -- /var/tmp/ben/ray-on-100 process >> bench_proc.txt
# N_CPUS=4 N_ENTDB=4 RAYON_NUM_THREADS=4 cargo run --release -- /var/tmp/ben/ray-on-100 process >> bench_proc.txt
# N_CPUS=8 N_ENTDB=4 RAYON_NUM_THREADS=8 cargo run --release -- /var/tmp/ben/ray-on-100 process >> bench_proc.txt
# N_CPUS=16 N_ENTDB=4 RAYON_NUM_THREADS=16 cargo run --release -- /var/tmp/ben/ray-on-100 process >> bench_proc.txt
# N_CPUS=32 N_ENTDB=4 RAYON_NUM_THREADS=32 cargo run --release -- /var/tmp/ben/ray-on-100 process >> bench_proc.txt
# N_CPUS=48 N_ENTDB=4 RAYON_NUM_THREADS=48 cargo run --release -- /var/tmp/ben/ray-on-100 process >> bench_proc.txt
# N_CPUS=64 N_ENTDB=4 RAYON_NUM_THREADS=64 cargo run --release -- /var/tmp/ben/ray-on-100 process >> bench_proc.txt

N_ENTDB=1 RAYON_NUM_THREADS=64 cargo run --release -- /var/tmp/ben/ray-on-100 thread >> bench_thread.txt
N_ENTDB=2 RAYON_NUM_THREADS=64 cargo run --release -- /var/tmp/ben/ray-on-100 thread >> bench_thread.txt
N_ENTDB=3 RAYON_NUM_THREADS=64 cargo run --release -- /var/tmp/ben/ray-on-100 thread >> bench_thread.txt
N_ENTDB=4 RAYON_NUM_THREADS=64 cargo run --release -- /var/tmp/ben/ray-on-100 thread >> bench_thread.txt
N_ENTDB=5 RAYON_NUM_THREADS=64 cargo run --release -- /var/tmp/ben/ray-on-100 thread >> bench_thread.txt
N_ENTDB=6 RAYON_NUM_THREADS=64 cargo run --release -- /var/tmp/ben/ray-on-100 thread >> bench_thread.txt
N_ENTDB=7 RAYON_NUM_THREADS=64 cargo run --release -- /var/tmp/ben/ray-on-100 thread >> bench_thread.txt
N_ENTDB=8 RAYON_NUM_THREADS=64 cargo run --release -- /var/tmp/ben/ray-on-100 thread >> bench_thread.txt
# N_ENTDB=16 cargo run --release -- /var/tmp/ben/ray-on-100 process >> bench_proc.txt
# N_ENTDB=24 cargo run --release -- /var/tmp/ben/ray-on-100 process >> bench_proc.txt
# N_ENTDB=32 cargo run --release -- /var/tmp/ben/ray-on-100 process >> bench_proc.txt
# N_ENTDB=48 cargo run --release -- /var/tmp/ben/ray-on-100 process >> bench_proc.txt
# N_ENTDB=48 cargo run --release -- /var/tmp/ben/ray-on-100 process >> bench_proc.txt