list:
    just --list

reset-containerd:
    sudo rm -rf /var/lib/containerd/io.containerd.metadata.v1.bolt
    sudo rm -rf /run/containerd/io.containerd.runtime.v2.task/default
    sudo systemctl restart containerd

reset-sky:
    rm -rf /tmp/sky-snapshots

reset-all: reset-containerd reset-sky

restart-containerd:
    sudo systemctl restart containerd

log-containerd:
    sudo systemctl status containerd

run: reset-all
    cargo run --release

clippy:
    cargo clippy --fix

run-registry:
    docker run --rm -p 5001:5000 --name registry registry:2
    # docker start registry

build-image:
    docker build -t localhost:5001/image:latest -f ../archive-image-splitter/Dockerfile ../archive-image-splitter
    docker push localhost:5001/image:latest

pull-image:
    # use ctr-remote instead, but nerdctl works as well
    # sudo ctr image rm localhost:5001/image:latest
    # sleep 1 # image rm seems to be async
    # sudo ctr image pull --snapshotter sky localhost:5001/image:latest

    # sudo nerdctl image rm localhost:5001/image:latest
    # sudo nerdctl image pull --snapshotter sky localhost:5001/image:latest

    sudo ctr-remote image rm localhost:5001/image:latest
    sleep 1
    sudo ctr-remote image rpull -snapshotter=sky -use-containerd-labels localhost:5001/image:latest
    sudo ctr-remote run -snapshotter=sky localhost:5001/image:latest running-image-id ls -lh

    # sudo crictl --runtime-endpoint=unix:///run/containerd/containerd.sock rmi localhost:5001/image:latest || true
    # sudo crictl --debug --runtime-endpoint=unix:///run/containerd/containerd.sock pull --annotation=io.containerd.cri.runtime-handler=sky localhost:5001/image:latest

bench:
    #!/usr/bin/env bash
    echo "=================="
    echo "Pulling image using overlayfs"
    sudo ctr-remote image rm localhost:5001/image:latest
    sleep 1
    sudo time -p ctr-remote image rpull -snapshotter=overlayfs -use-containerd-labels localhost:5001/image:latest

    echo "=================="
    echo "Pulling image using sky"
    sudo ctr-remote image rm localhost:5001/image:latest
    sleep 1
    sudo time -p ctr-remote image rpull -snapshotter=sky -use-containerd-labels localhost:5001/image:latest

bench-ray:
    sudo ctr-remote image rm localhost:5001/ray-ml:latest
    sleep 1
    time sudo ctr-remote image rpull -snapshotter=sky -use-containerd-labels localhost:5001/ray-ml:latest

    sudo ctr-remote image rm localhost:5001/ray-ml:latest
    sleep 1
    time sudo ctr-remote image rpull -snapshotter=overlayfs -use-containerd-labels localhost:5001/ray-ml:latest

pull-vicuna:
    sudo ctr-remote image rm localhost:5000/vicuna:edbbe5
    sleep 1
    sudo ctr-remote image rpull -snapshotter=sky -use-containerd-labels localhost:5000/vicuna:edbbe5

run-vicuna:
    sudo ctr-remote run -snapshotter=sky localhost:5000/vicuna:7aad12 running-image-id-6 /usr/bin/ls -lh vicuna-13b-1.1

pull-azure-vicuna:
    docker image rm ucbskycontainers.azurecr.io/vicuna:latest
    time docker pull ucbskycontainers.azurecr.io/vicuna:latest