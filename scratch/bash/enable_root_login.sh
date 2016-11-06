# This code enables the root user to login and allows passwords to be used
#  for SSH.  The main use case is an EC2 instance where root login is
#  disabled and only .pem key authentication is allowed
#  For EC2, we need to modify SSH config, set password, and bounce sshd 

sed -i 's/PermitRootLogin no/PermitRootLogin yes/g' /etc/ssh/sshd_config
sed -i 's/#PasswordAuthentication no/PasswordAuthentication yes/g' /etc/ssh/sshd_config
sed -i 's/PasswordAuthentication no/PasswordAuthentication yes/g' /etc/ssh/sshd_config
echo -e "newpassword\nnewpassword" | (passwd --stdin $USER)
service sshd restart
