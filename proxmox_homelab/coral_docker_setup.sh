# Update Proxmox VE
apt-get update && apt-get -y upgrade
apt install pv vim

# Install post-pve-install.sh
bash -c "$(curl -fsSL https://raw.githubusercontent.com/community-scripts/ProxmoxVE/main/tools/pve/post-pve-install.sh)"

# Install microcode of the post intall https://github.com/tteck/Proxmox/blob/main/misc/microcode.sh
bash -c "$(wget -qLO - https://cdn.jsdelivr.net/gh/tteck/Proxmox@main/misc/microcode.sh)"

# Install Home Assistant VM
wget -o - https://github.com/tteck/Proxmox/raw/main/vm/haos-vm.sh
sed -i 's/pve-manager\/8\.\[1-3\]/pve-manager\/9.[1-3]/g' haos-vm.sh
bash haos-vm.sh

# Install Docker CT
bash -c "$(curl -fsSL https://git.community-scripts.org/community-scripts/ProxmoxVE/raw/branch/main/ct/docker.sh)"

# bypass to passwd to the docker
pct exec 100 bash

# Install Coral Edge TPU & Gasket Driver
lspci -vv | grep MSI-X
apt update && apt upgrade -y
echo "deb https://packages.cloud.google.com/apt coral-edgetpu-stable main" | sudo tee /etc/apt/sources.list.d/coral-edgetpu.list
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -
wget -O- https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/coral-edgetpu.gpg
apt update && apt upgrade -y
apt-get install libedgetpu1-std -y

apt remove gasket-dkms -y
apt install git devscripts dh-dkms dkms -y
git clone https://github.com/google/gasket-driver.git
cd gasket-driver/
debuild -us -uc -tc -b
cd ..
apt install pve-headers -y
dpkg -i gasket-dkms_1.0-18_all.deb
cd
apt update && apt upgrade -y
dpkg --configure gasket-dkms
dkms status gasket
lspci -nn | grep 089a
ls /dev/apex_0

# 1. Fixed /usr/src/gasket-1.0/gasket_page_table.c (line 57)
# Issue: MODULE_IMPORT_NS(DMA_BUF) had incorrect syntax
# Fix: Changed to MODULE_IMPORT_NS("DMA_BUF") - the macro requires a string literal

# 2. Fixed /usr/src/gasket-1.0/gasket_core.c (line 1376)
# Issue: no_llseek function has been deprecated in kernel 6.17
# Fix: Changed to noop_llseek - the modern replacement function


# Mount CCTV clips directory to Docker CT
# mp0: local-lvm:vm-101-disk-1,mp=/cctv_clips,backup=1,size=300G
mkdir /data
mkdir /data/cctv_clips
pct set 100 -mp0 storage-lvm:vm-100-disk-0,mp=/cctv_clips,backup=1,size=200G
ls /data/cctv_clips/
vim /etc/pve/lxc/100.conf
printf "\nlxc.mount.entry: /dev/dri/renderD128 dev/dri/renderD128 none bind,optional,create=file 0, 0\nlxc.mount.entry: /dev/apex_0 dev/apex_0 none bind,optional,create=file 0,0\n" >> /etc/pve/lxc/100.conf
