#  This snippet sets ulimits for a CentOS host.  The use case is with Alpine
#  Chrous or Greenplum DB.  In addition the the limits.conf there is often
#  a file that overrides limits, so we need to delete this as well

printf "* soft nofile 65536\n* hard nofile 65536\n* soft nproc 131072\n* hard nproc 131072" >> /etc/security/limits.conf
sed -i '/*/Id' /etc/security/limits.d/*0-nproc.conf
