#!/bin/bash

addrs=( 10.0.1.196 10.0.1.134 10.0.1.73 10.0.1.12 10.0.1.141 10.0.1.111 10.0.1.114 10.0.1.18 10.0.1.152 10.0.1.248 10.0.1.56 10.0.1.153 10.0.1.185 10.0.1.188 )

var=0
for i in "${addrs[@]}"
do
	fname="/etc/sysconfig/network-scripts/ifcfg-eth0:$var"

	echo "$fname"
	read -r -d '' text <<-EOF
	DEVICE=eth0:$var
	BOOTPROTO=static
	IPADDR=$i
	NETMASK=255.255.255.0
	ONBOOT=yes
	EOF

	echo "$text" | sudo tee "$fname"
	let "var=var+1"
done

sudo service network restart
ifconfig

