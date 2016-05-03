# -*- mode: ruby -*-
# vi: set ft=ruby :
Vagrant.configure(2) do |config|
  config.vm.box = "ubuntu/trusty64"
  config.vm.synced_folder ".", "/opt/multisort-median"
  config.vm.network "private_network", ip: "10.0.0.109"
  config.vm.hostname = "multisort-median.vagrant"

  # install golang 1.6
  config.vm.provision "shell", inline: <<-SHELL
    set -e
    mkdir -p /opt/go
    chown -R vagrant:vagrant /opt/go
    curl https://storage.googleapis.com/golang/go1.6.linux-amd64.tar.gz > /tmp/go1.6.linux-amd64.tar.gz
    tar -xf /tmp/go1.6.linux-amd64.tar.gz -C /usr/local
    ln -s /usr/local/go/bin/* /usr/local/bin
  SHELL
end
