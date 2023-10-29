set -x

INDIR=/var/tmp/ben/ray-on-100
BATCH_SIZE=32
# ENT_LIMIT=1000
ENT_LIMIT=2000

# for j in 1 2 3 4 5
# do
#     for i in 1 4 8 12 16 20 24 28 32 64 72
#     do
#         N_CPUS=$i N_ENTDB=1 cargo run --release -- $INDIR $ENT_LIMIT process >> bench_process.txt
#     done
# done

# BATCH_SIZE=32
# ENT_LIMIT=2000

# for i in 1 2 3 4 5
# do
#     for j in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16
#     do
#         # N_ENTDB=$j BATCH_SIZE=$BATCH_SIZE cargo run --release -- $INDIR $ENT_LIMIT thread >> bench_thread_client_mutex.txt
#         N_ENTDB=$j BATCH_SIZE=$BATCH_SIZE cargo run --release -- $INDIR $ENT_LIMIT thread >> bench_thread_client_threadlocal.txt
#     done
# done

# for BATCH_SIZE in 16 32 64 128 256
for i in 1 2 3 4 5
do
    for j in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16
    do
        N_ENTDB=$j BATCH_SIZE=$BATCH_SIZE cargo run --release -- $INDIR $ENT_LIMIT thread >> bench_thread_client_threadlocal_2.txt
    done
done