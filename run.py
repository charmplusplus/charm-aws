from aws import CharmCloudManager

charm_aws_launcher = CharmCloudManager('/home/aditya/charm.pem', 'us-east-2')

ami_id = 'ami-09a7ebfe5e3718850'
workdir = '/home/ec2-user/charm/examples/charm++/shrink_expand/jacobi2d-iter'
#command = f'{workdir}/charmrun_elastic +p%(num_pes)s {workdir}/jacobi2d 8192 256 5000 +balancer GreedyCentralLB +LBDebug 3 ++nodelist /tmp/nodelist ++server ++server-port 1234 +LBTestPESpeed'
#workdir = '/home/ec2-user/charm/examples/charm++/speeds'
#command1 = f'{workdir}/charmrun_elastic +p%(num_pes)s {workdir}/speed %(num_pes)s ++nodelist /tmp/nodelist +LBTestPESpeed'
command1 = f'{workdir}/charmrun_elastic +p%(num_pes)s {workdir}/jacobi2d 16384 512 1000 ++nodelist /tmp/nodelist ++server ++server-port 1234'
command2 = f'{workdir}/charmrun_elastic +p%(num_pes)s {workdir}/jacobi2d 16384 512 1000 +balancer GreedyRefineLB +LBDebug 3 ++nodelist /tmp/nodelist ++server ++server-port 1234 +LBTestPESpeed'
command3 = f'{workdir}/charmrun_elastic +p%(num_pes)s {workdir}/jacobi2d 16384 512 1000 +balancer GreedyRefineLB +LBDebug 3 ++nodelist /tmp/nodelist ++server ++server-port 1234'
commands = [command1, command2, command3]

# ["c5.large", "c5.xlarge", "c5.2xlarge", "c5.4xlarge", "c5.9xlarge", "c5.12xlarge", "c5.18xlarge", "c5.24xlarge", "c5.metal",
#     "c5a.large", "c5a.xlarge", "c5a.2xlarge", "c5a.4xlarge", "c5a.8xlarge", "c5a.12xlarge", "c5a.16xlarge", "c5a.24xlarge",
#     "c5ad.large", "c5ad.xlarge", "c5ad.2xlarge", "c5ad.4xlarge", "c5ad.8xlarge", "c5ad.12xlarge", "c5ad.16xlarge", "c5ad.24xlarge",
#     "c5d.large", "c5d.xlarge", "c5d.2xlarge", "c5d.4xlarge", "c5d.9xlarge", "c5d.12xlarge", "c5d.18xlarge", "c5d.24xlarge", "c5d.metal",
#     "c4.large", "c4.xlarge", "c4.2xlarge", "c4.4xlarge", "c4.8xlarge"]

instance_types = [
    "c6g.medium",
    "c6g.large",
    "c6g.xlarge",
    "c6g.2xlarge",
    "c6g.4xlarge",
    "c6g.8xlarge",
    "c6g.12xlarge",
    "c6g.16xlarge",
    "c6g.metal",
    "c7g.medium",
    "c7g.large",
    "c7g.xlarge",
    "c7g.2xlarge",
    "c7g.4xlarge",
    "c7g.8xlarge",
    "c7g.12xlarge",
    "c7g.16xlarge",
    "c7g.metal",
    "c6gn.medium",
    "c6gn.large",
    "c6gn.xlarge",
    "c6gn.2xlarge",
    "c6gn.4xlarge",
    "c6gn.8xlarge",
    "c6gn.12xlarge",
    "c6gn.16xlarge",
]

charm_aws_launcher.run(
    ami_id, instance_types, 'charm-example',
    commands, total_target_capacity=32, on_demand_count=1,
    key_name='charm',
    subnet_ids=['subnet-0a9bd0e60a998d409'],
    security_group_ids=['sg-01d547bcbf0ba3c13']
)


#charm_aws_launcher.terminate_fleet('fleet-718eba2e-51b5-69b4-8c90-2e22ceae7a5b')
#charm_aws_launcher.terminate_fleet('fleet-68495d6b-5547-4bbe-a0d9-f8a0a893e05f')