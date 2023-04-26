list:
    just --list

restart-containerd:
    sudo systemctl restart containerd

log-containerd:
    sudo systemctl status containerd

run:
    cargo run --release

clippy:
    cargo clippy --fix

run-registry:
    # docker run -d -p 5000:5000 --name registry registry:2
    docker start registry

build-image:
    docker build -t localhost:5000/image:latest -f ../image-splitter/Dockerfile ../image-splitter
    docker push localhost:5000/image:latest

pull-image:
    # use ctr-remote instead, but nerdctl works as well
    # sudo ctr image rm localhost:5000/image:latest
    # sleep 1 # image rm seems to be async
    # sudo ctr image pull --snapshotter sky localhost:5000/image:latest

    # sudo nerdctl image rm localhost:5000/image:latest
    # sudo nerdctl image pull --snapshotter sky localhost:5000/image:latest

    sudo ctr-remote image rm localhost:5000/image:latest
    sleep 1
    sudo ctr-remote image rpull -snapshotter=sky -use-containerd-labels localhost:5000/image:latest
    # sudo ctr-remote run -snapshotter=sky localhost:5000/image:latest ls

    # sudo crictl --runtime-endpoint=unix:///run/containerd/containerd.sock rmi localhost:5000/image:latest || true
    # sudo crictl --debug --runtime-endpoint=unix:///run/containerd/containerd.sock pull --annotation=io.containerd.cri.runtime-handler=sky localhost:5000/image:latest