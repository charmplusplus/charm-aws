from aws import CharmAWSManager

charm_aws_launcher = CharmAWSManager('/home/aditya/keypair-msi.pem')

ami_id = 'ami-00196fe4b7de060d7'
workdir = '/home/ec2-user/charm/examples/charm++/shrink_expand/jacobi2d-iter'
command = f'time {workdir}/charmrun +p%(num_pes)s {workdir}/jacobi2d 16384 256 20000 +balancer GreedyCentralLB +LBDebug 3 ++nodelist /tmp/nodelist ++server ++server-port 1234 ++verbose +LBTestPESpeed'

#["c5.large", "c5.xlarge", "c5.2xlarge", "c5.4xlarge", "c5.9xlarge", "c5.12xlarge", "c5.18xlarge", "c5.24xlarge", "c5.metal",
    # "c5a.large", "c5a.xlarge", "c5a.2xlarge", "c5a.4xlarge", "c5a.8xlarge", "c5a.12xlarge", "c5a.16xlarge", "c5a.24xlarge",
    # "c5ad.large", "c5ad.xlarge", "c5ad.2xlarge", "c5ad.4xlarge", "c5ad.8xlarge", "c5ad.12xlarge", "c5ad.16xlarge", "c5ad.24xlarge",
    # "c5d.large", "c5d.xlarge", "c5d.2xlarge", "c5d.4xlarge", "c5d.9xlarge", "c5d.12xlarge", "c5d.18xlarge", "c5d.24xlarge", "c5d.metal",
    # "c4.large", "c4.xlarge", "c4.2xlarge", "c4.4xlarge", "c4.8xlarge"]

charm_aws_launcher.run(
    ami_id, ["c5.large", "c5.xlarge", "c5.2xlarge", "c5.4xlarge", "c5.9xlarge", "c5.12xlarge", "c5.18xlarge", "c5.24xlarge", "c5.metal",
    "c5a.large", "c5a.xlarge", "c5a.2xlarge", "c5a.4xlarge", "c5a.8xlarge", "c5a.12xlarge", "c5a.16xlarge", "c5a.24xlarge",
    "c5ad.large", "c5ad.xlarge", "c5ad.2xlarge", "c5ad.4xlarge", "c5ad.8xlarge", "c5ad.12xlarge", "c5ad.16xlarge", "c5ad.24xlarge",
    "c5d.large", "c5d.xlarge", "c5d.2xlarge", "c5d.4xlarge", "c5d.9xlarge", "c5d.12xlarge", "c5d.18xlarge", "c5d.24xlarge", "c5d.metal",
    "c4.large", "c4.xlarge", "c4.2xlarge", "c4.4xlarge", "c4.8xlarge"], 'charm-example',
    command, total_target_capacity=64, on_demand_count=4,
    key_name='keypair-msi',
    subnet_ids=['subnet-0669cf9d5310b06c2'],
    security_group_ids=['sg-068f84aea39ed365a']
)


#charm_aws_launcher.terminate_fleet('fleet-718eba2e-51b5-69b4-8c90-2e22ceae7a5b')
#charm_aws_launcher.terminate_fleet('fleet-68495d6b-5547-4bbe-a0d9-f8a0a893e05f')