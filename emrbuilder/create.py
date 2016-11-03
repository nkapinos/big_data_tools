"""
Author:  Nate Kapinos
EMR Cluster Builder
This script creates an Amazon EMR cluster given an input configuation:

{
   "aws_access_key": "ABCDEFGHIJ",
   "aws_secret_key": "ABCDEFHIJ",
   "aws_security_group_id": "SG-12345",
   "aws_region": "us-west-1",
   "emr_version": "5.0",
   "emr_master_node_count": 1,
   "emr_master_node_type": "m4.large",
   "emr_core_node_count": 2,
   "emr_core_node_type": "m4.large",
   "emr_task_node_count": 0,
   "emr_task_node_type": "m4.large",
   "emr_market_type": "ON_DEMAND",
   "emr_market_bid_price": ".05",
   "emr_cluster_name": "Large EMR Cluster"
   "emr_log_uri": "s3://alpine-qa/emr_automation_logs",
   "ec2_keyname": "adl-performance"
}

To use a configuration, create a file with this format in teh 'configs' directory
and set CLUSTER_CONF_FILE environment variable:
export CLUSTER_CONF_FILE=emr_default.json

"""

import boto
import boto.emr
import json, os, sys, time, random, string
from boto.emr.connection import EmrConnection
from boto.regioninfo import RegionInfo
from boto.emr.instance_group import InstanceGroup
from boto.emr.step import JarStep
from boto import connect_route53
from boto.route53.record import ResourceRecordSets

lib_path = os.path.abspath(os.path.join("..", "..", ".."))
sys.path.append(lib_path)
import clusterbuilder.lib.ConfigReader as config_reader

def create_emr_cluster(cr):
    """
    @PARAM:  Cluster configuration reader object
    Creates an EMR cluster given a set of configuration parameters
    Return:  EMR Cluster ID
    """

    #region = cr.get_config("aws_region")
    #conn = boto.emr.connect_to_region(region)
    conn = EmrConnection(
        cr.get_config("aws_access_key"),
        cr.get_config("aws_secret_key"),
        region = RegionInfo(name = cr.get_config("aws_region"),
                            endpoint = cr.get_config("aws_region") + ".elasticmapreduce.amazonaws.com" ))


    #  Create list of instance groups:  master, core, and task
    instance_groups = []
    instance_groups.append(InstanceGroup(
        num_instances = cr.get_config("emr_master_node_count"),
        role = "MASTER",
        type = cr.get_config("emr_master_node_type"),
        market = cr.get_config("emr_market_type"),
        name = "Master Node" ))

    instance_groups.append(InstanceGroup(
        num_instances = cr.get_config("emr_core_node_count"),
        role = "CORE",
        type = cr.get_config("emr_core_node_type"),
        market = cr.get_config("emr_market_type"),
        name = "Core Node" ))

    #  Only create task nodes if specifcally asked for
    if cr.get_config("emr_task_node_count") > 0:
        instance_groups.append(InstanceGroup(
            num_instances = cr.get_config("emr_task_node_count"),
            role = "TASK",
            type = cr.get_config("emr_task_node_type"),
            market = cr.get_config("emr_market_type"),
            name = "Task Node" ))

    print "Creating EMR Cluster with instance groups: {0}".format(instance_groups)

    #  Use these params to add overrrides, these will go away in Boto3
    api_params = {"Instances.Ec2SubnetId": cr.get_config("aws_subnet_id"), "ReleaseLabel": cr.get_config("emr_version")}

    #  Add step to load data
    step_args = ["s3-dist-cp","--s3Endpoint=s3-us-west-1.amazonaws.com","--src=s3://alpine-qa/automation/automation_test_data/","--dest=hdfs:///automation_test_data","--srcPattern=.*[a-zA-Z,]+"]
    step = JarStep(name = "s3distcp for data loading",
                jar = "command-runner.jar",
                step_args = step_args,
                action_on_failure = "CONTINUE"
                )

    cluster_id = conn.run_jobflow(
        cr.get_config("emr_cluster_name"),
        instance_groups = instance_groups,
        action_on_failure = "TERMINATE_JOB_FLOW",
        keep_alive = True,
        enable_debugging = True,
        log_uri = cr.get_config("emr_log_uri"),
        #hadoop_version = "Amazon 2.7.2",
        #ReleaseLabel = "emr-5.0.0",
        #ami_version = "5.0.0",
        steps = [step],
        bootstrap_actions = [],
        ec2_keyname = cr.get_config("ec2_keyname"),
        visible_to_all_users = True,
        job_flow_role = "EMR_EC2_DefaultRole",
        service_role = "EMR_DefaultRole",
        api_params = api_params )

    print "EMR Cluster created, cluster id: {0}".format(cluster_id)
    state = conn.describe_cluster(cluster_id).status.state
    while state != u'COMPLETED' and state != u'SHUTTING_DOWN' and state != u'FAILED' and state != u'WAITING':
        #sleeping to recheck for status.
        time.sleep(5)
        state = conn.describe_cluster(cluster_id).status.state
        print "State is: {0}, sleeping 5s...".format(state)

    if state == u'SHUTTING_DOWN' or state == u'FAILED':
        return "ERROR"

    #Check if the state is WAITING. Then launch the next steps
    if state == u'WAITING':
        #Finding the master node dns of EMR cluster
        master_dns = conn.describe_cluster(cluster_id).masterpublicdnsname
        print "DNS Name: {0}".format(master_dns)
        return cluster_id

def create_data_source_variable(cluster_id, cr):
    """
    Creates a data source variable .json file using the cluster_id of an EMR cluster_id
    @PARAM:  cluster_id:  ID of an EMR cluster
    return:  True if success, creates a file in the pwd 'default_emr.json'

    Object created should look like:

    HADOOP_DATA_SOURCE_NAME="emr_data_source"
    HADOOP_DATA_SOURCE_DISTRO="Cloudera CDH5.4-5.7"
    HADOOP_DATA_SOURCE_HOST="emr_master_dns_hostname"
    HADOOP_DATA_SOURCE_PORT=8020
    HADOOP_DATA_SOURCE_USER="hdfs"
    HADOOP_DATA_SOURCE_GROUP="hadoop"
    HADOOP_DATA_SOURCE_JT_HOST="emr_master_dns_hostname"
    HADOOP_DATA_SOURCE_JT_PORT=8032
    CONNECTION_PARAMETERS='[{"key":"mapreduce.jobhistory.address", "value":"0.0.0.0:10020"}, ' \
                            '{"key":"mapreduce.jobhistory.webapp.address", "value":"cdh5hakerberosnn.alpinenow.local:19888"}, ' \
                            '{"key":"yarn.app.mapreduce.am.staging-dir", "value":"/tmp/hadoop-yarn/staging"}, ' \
                            '{"key":"yarn.resourcemanager.admin.address", "value":"cdh5hakerberosnn.alpinenow.local:8033"}, ' \
                            '{"key":"yarn.resourcemanager.resource-tracker.address", "value":"cdh5hakerberosnn.alpinenow.local:8031"}, ' \
                            '{"key":"yarn.resourcemanager.scheduler.address", "value":"cdh5hakerberosnn.alpinenow.local:8030"}]'

    """
    conn = EmrConnection(
        cr.get_config("aws_access_key"),
        cr.get_config("aws_secret_key"),
        region = RegionInfo(name = cr.get_config("aws_region"),
            endpoint = cr.get_config("aws_region") + ".elasticmapreduce.amazonaws.com" ))

    emr_cluster = conn.describe_cluster(cluster_id)
    master_dns_hostname = emr_cluster.masterpublicdnsname

    # Build up connection parameters
    conn_params = []
    conn_params.append({"key": "mapreduce.jobhistory.address", "value": "{0}:10020".format(master_dns_hostname)})
    conn_params.append({"key": "mapreduce.jobhistory.webapp.address", "value": "{0}:19888".format(master_dns_hostname)})
    conn_params.append({"key": "yarn.app.mapreduce.am.staging-dir", "value": "/user"})
    conn_params.append({"key": "yarn.resourcemanager.admin.address", "value": "{0}:8033".format(master_dns_hostname)})
    conn_params.append({"key": "yarn.resourcemanager.scheduler.address", "value": "{0}:8030".format(master_dns_hostname)})
    conn_params_str = "CONNECTION_PARAMETERS=\"{0}\"".format(conn_params)
    email_str = "EMAIL=\"avalanche_{0}.alpinenow.com\"".format(random.randint(1,99999))

    with open("emr_default.conf", "w") as f:
        f.writelines("HADOOP_DATA_SOURCE_NAME=\"{0}\"\n".format(cr.get_config("emr_cluster_name")))
        f.writelines("HADOOP_DATA_SOURCE_DISTRO=\"{0}\"\n".format("Amazon EMR5"))
        f.writelines("HADOOP_DATA_SOURCE_HOST=\"{0}\"\n".format(master_dns_hostname))
        f.writelines("HADOOP_DATA_SOURCE_POST=\"8020\"\n")
        f.writelines("HADOOP_DATA_SOURCE_USER=\"hdfs\"\n")
        f.writelines("HADOOP_DATA_SOURCE_GROUP=\"hadoop\"\n")
        f.writelines("HADOOP_DATA_SOURCE_JT_HOST=\"{0}\"\n".format(master_dns_hostname))
        f.writelines("HADOOP_DATA_SOURCE_JT_PORT=\"8032\"\n")
        f.writelines(email_str)
        f.writelines(conn_params_str)

def get_internal_ips_from_emr(cluster_id, cr):
    """
    Retrieves a list of internal IP addresses for a given EMR cluster
    """

    #  Open connection to EMR
    conn = EmrConnection(
        cr.get_config("aws_access_key"),
        cr.get_config("aws_secret_key"),
        region = RegionInfo(name = cr.get_config("aws_region"),
            endpoint = cr.get_config("aws_region") + ".elasticmapreduce.amazonaws.com" ))

    #  Build list of internal ips from list_instances EMR API
    emr_internal_ips = []
    emr_instances = conn.list_instances(cluster_id).instances
    for instance in emr_instances:
        emr_internal_ips.append(instance.privateipaddress)

    return emr_internal_ips

def add_route53_record(emr_internal_ips, cr):
    """
    Given a list of IP addressed, create A records in a given domain
    """

    conn = connect_route53(aws_access_key_id = cr.get_config("aws_access_key"), aws_secret_access_key = cr.get_config("aws_secret_key"))

    zone = conn.get_zone("alpinenow.local")

    print "Adding DNS Records for: {0}".format(emr_internal_ips)
    for ip in emr_internal_ips:
        internal_dns = "ip-" + ip.replace(".", "-") + ".alpinenow.local"
        response = zone.add_a(internal_dns, ip)  #  TODO:  Do something with response


if __name__ == "__main__":
    #  Meat and Potatoes

    print "1.  Reading configuration..."
    cr = config_reader.ConfigReader()

    print "2.  Creating EMR Cluster..."
    cluster_id = create_emr_cluster(cr)

    print "3.  Add Route53 entries..."
    emr_internal_ips = []
    emr_internal_ips = get_internal_ips_from_emr(cluster_id, cr)
    add_route53_record(emr_internal_ips, cr)

    print "4.  Creating data source config file..."
    create_data_source_variable(cluster_id, cr)
