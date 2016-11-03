#!/usr/bin/python

"""
Baker Deploy by Nate Kapinos
.1  9/29/2016 - Initial Version
.2  10/6/2016 - Execute using Fabric instead of paramiko SSH, tag instance, ping host for availability

Bake creates an EC2 instance, and carries out a series of steps (receipes) on it.
Receipes are written in bash, and meant to emulate the command-line configuration done by a human.
Recipes are executed by fabric, and require the target host have root access

Example:
    bake.py simplechorus rserver
    This will create a chorus611 host with just an rserver

    bake.py chorus611

"""

import argparse, json, os, time, logging
import boto.ec2
from os import listdir
from os.path import isfile, join
from fabric.api import run, sudo, settings
from fabric.tasks import execute
from fabric.api import env
import subprocess
import requests


def read_config():
    """
    Reads a JSON formatted config file for Baker.  This config contains AWS configration
    - secret keys, instance type, instance disk size, etc.
    :return:
    """

    try:
        baker_config = os.environ['BAKER_CONFIG']
    except KeyError:
        baker_config = "default.json"

    with open("config/{0}".format(baker_config), 'r') as f:
        baker_json = json.loads(f.read())

    logging.info("Successfully read configuration {0}".format(baker_config))
    return baker_json


def read_recipe(recipe_steps, recipe):
    """

    :param recipe: file handle with steps to perform, one step per line
    :return: list of steps
    """

    #  Check if recipe exists
    recipe_list = [f for f in listdir("recipes/") if isfile(join("recipes/", f))]
    if not recipe in recipe_list:
        raise Exception("Recipe {0} not found in {1}/recipes/".format(recipe, os.getcwd()))

    with open("recipes/{0}".format(recipe), "r") as f:
        new_recipe_steps = f.readlines()

    for step in new_recipe_steps:
        recipe_steps.append(step.strip("\n"))

    return recipe_steps


def create_aws_instance(baker_config, instance_tag):
    """
    Creates an EC2 instance given parameters
    :param baker_config: Dictionary containing Key/Value pairs for all necessary information to create instance
    :return:
    """

    conn = boto.ec2.connect_to_region(baker_config['aws_region'], aws_access_key_id=baker_config['aws_access_key'],
                                      aws_secret_access_key=baker_config['aws_secret_key'])

    reservation = conn.run_instances(baker_config['aws_ami_id'], key_name=baker_config['aws_key_name'],
                                     instance_type=baker_config['aws_instance_type'],
                                     subnet_id=baker_config['aws_subnet_id'],
                                     security_group_ids=[baker_config['aws_security_group']])

    instance = reservation.instances[0]
    conn.create_tags([instance.id], {"Name": "bakerdeploy-{0}-{1}".format(instance_tag, instance.private_ip_address)})

    while instance.update() != "running":
        time.sleep(5)

    print "Instance Started with ID: {0}".format(instance.id)
    print "Instance External IP: {0}".format(instance.ip_address)
    print "Instance Internal IP: {0}".format(instance.private_ip_address)

    retries = 0
    command = ["ping", "-c", "1", "{0}".format(instance.private_ip_address)]
    while retries < 60:
        rc = subprocess.call(command)
        if rc == 0:
            print "Host on {0} is UP. Sleeping 5 seconds for SSH access...".format(instance.private_ip_address)
            time.sleep(5)
            break
        else:
            retries += 1

    return instance


def fabric_run_step(step):
    run(step)


def fabric_sudo_step(step, user):
    sudo(step, user=user)


def remote_execute_steps(host, recipe_steps):
    """

    :param host:
    :param recipe_steps:
    :return:
    """

    env.host_string = host
    env.user = "root"
    env.key_filename = "assets/adl-performance.pem"
    for step in recipe_steps:
        if step.startswith("#"):
            print "Skipping step: {0}".format(step)
            continue
        else:
            print "Running step: {0}".format(step)
            execute(fabric_run_step, step, hosts=[host])

def check_host_accessible(host):
    """

    :param host:
    :return:
    """

    retries = 0
    while retries < 120:
        response = requests.get("http://{0}:8080".format(host))
        if response.status_code == 200:
            print "Host {0} is accessible..".format(host)
            return True
        else:
            print "Host {0} is not accessible: {1}, sleeping 1s...".format(host, response.status_code)
            time.sleep(1)
            retries += 1


def write_host_config(ip):
    """

    :param ip:
    :return:
    """

    config = {"host": ip,
                  "chorus_port": "8080",
                  "alpine_port": "9090",
                  "host_username": "root",
                  "host_password": "alpineRocks",
                  "alpine_install_dir": "/usr/local/chorus",
                  "chorus_data_dir": "/data/chorus",
                  "home_directory": "/home/chorus",
                  "notebook_host": "10.10.0.199",
                  "notebook_port": "8000"}

    with open("baker.conf", "wb") as f:
        f.write(json.dumps(config))


if __name__ == "__main__":

    # parse command line actions
    parser = argparse.ArgumentParser(description="Baker Deployment")
    parser.add_argument(" ", nargs="+", type=str)
    args = vars(parser.parse_args())[" "]

    # 1.  Read config
    baker_config = read_config()

    # 2.  Create instance
    instance_tag = ""
    for item in args:
        instance_tag += item
    instance = create_aws_instance(baker_config, instance_tag)

    # 3.  Read in recipe file to list of steps
    recipe_steps = []
    for arg in args:
        recipe_steps = read_recipe(recipe_steps, arg)

    # 4.  Execute commands on remote host
    remote_execute_steps(instance.private_ip_address, recipe_steps)
    if not check_host_accessible(instance.private_ip_address):
        raise Exception("Host {0} failed to start...".format(instance.private_ip_address))

    # 5.  Write host config
    write_host_config(instance.private_ip_address)
