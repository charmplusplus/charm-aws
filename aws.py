import boto3
import base64
import time
import paramiko
import sys
import asyncio
import asyncssh
import json
from datetime import datetime, timedelta


class CharmCloudManager:
    """A class to launch and manage AWS EC2 instances for Charm++ applications."""
    
    def __init__(self, key_path, region_name='us-east-1'):
        """
        Initialize the CharmCloudManager.
        
        Args:
            region_name (str): AWS region to use (default: 'us-east-1')
        """
        self.key_path = key_path
        self.region_name = region_name
        self.active_instances = []
        self.interrupted_instances = []

    def create_placement_group(self, group_name, strategy='cluster'):
        """
        Create a placement group with the specified strategy.
        
        Args:
            group_name (str): Name for the placement group
            strategy (str): Placement strategy ('cluster', 'spread', or 'partition')
            
        Returns:
            str: Name of the created placement group
        """
        ec2_client = boto3.client('ec2', region_name=self.region_name)
        
        try:
            ec2_client.create_placement_group(
                GroupName=group_name,
                Strategy=strategy
            )
            print(f"Created placement group: {group_name} with strategy: {strategy}")
            return group_name
        except ec2_client.exceptions.ClientError as e:
            if 'already exists' in str(e):
                print(f"Placement group {group_name} already exists")
                return group_name
            else:
                print(f"Error creating placement group: {e}")
                raise

    def create_launch_template(
            self,
            template_name,
            ami_id,
            instance_type,
            key_name=None,
            security_group_ids=None,
            placement_group=None,
            user_data=None,
            iam_instance_profile=None,
            ebs_optimized=False,
            network_interfaces=None,
            tags=None
    ):
        """
        Create a launch template with specified parameters including placement group.
        
        Args:
            template_name (str): Name for the launch template
            ami_id (str): AMI ID to use
            instance_type (str): Instance type
            key_name (str): SSH key pair name
            security_group_ids (list): List of security group IDs
            placement_group (str): Placement group name
            user_data (str): User data script (will be base64 encoded)
            iam_instance_profile (str): IAM instance profile name or ARN
            ebs_optimized (bool): Whether to enable EBS optimization
            network_interfaces (list): Network interface specifications
            tags (list): List of tags for the instance
            
        Returns:
            str: ID of the created launch template
        """
        ec2_client = boto3.client('ec2', region_name=self.region_name)
        
        # Prepare launch template data
        launch_template_data = {
            'ImageId': ami_id,
            'InstanceType': instance_type,
            'EbsOptimized': ebs_optimized
        }
        
        # Add placement group if specified
        if placement_group:
            launch_template_data['Placement'] = {
                'GroupName': placement_group
            }
        
        # Add optional parameters if provided
        if key_name:
            launch_template_data['KeyName'] = key_name
        
        if security_group_ids:
            launch_template_data['SecurityGroupIds'] = security_group_ids
        
        if user_data:
            # Base64 encode the user data
            encoded_user_data = base64.b64encode(
                user_data.encode('utf-8')).decode('utf-8')
            launch_template_data['UserData'] = encoded_user_data
        
        if iam_instance_profile:
            if iam_instance_profile.startswith('arn:'):
                launch_template_data['IamInstanceProfile'] = {
                    'Arn': iam_instance_profile
                }
            else:
                launch_template_data['IamInstanceProfile'] = {
                    'Name': iam_instance_profile
                }
        else:
            # Create instance profile
            iam_client = boto3.client('iam', region_name=self.region_name)
            try:
                instance_profile = iam_client.create_instance_profile(
                    InstanceProfileName='charm-instance-profile'
                )

                # Attach role to instance profile
                iam_client.add_role_to_instance_profile(
                    InstanceProfileName='charm-instance-profile',
                    RoleName='ec2-hpc'  # Use your existing role name
                )

                launch_template_data['IamInstanceProfile'] = {
                    'Name': 'charm-instance-profile'
                }
            except:
                print("Instance profile already exists or role is already attached")

        
        if network_interfaces:
            launch_template_data['NetworkInterfaces'] = network_interfaces
        
        if tags:
            launch_template_data['TagSpecifications'] = [
                {
                    'ResourceType': 'instance',
                    'Tags': tags
                }
            ]
        
        try:
            response = ec2_client.create_launch_template(
                LaunchTemplateName=template_name,
                VersionDescription='Initial version',
                LaunchTemplateData=launch_template_data
            )
            
            template_id = response['LaunchTemplate']['LaunchTemplateId']
            print(f"Created launch template: {template_name} (ID: {template_id})")
            return template_id
        except ec2_client.exceptions.ClientError as e:
            if 'already in use' in str(e):
                print(f"Launch template {template_name} already exists")
                response = ec2_client.describe_launch_templates(
                    LaunchTemplateNames=[template_name]
                )
                template_id = response['LaunchTemplates'][0]['LaunchTemplateId']
                return template_id
            else:
                print(f"Error creating launch template: {e}")
                raise

    def launch(
            self,
            launch_template_id,
            total_target_capacity,
            on_demand_count=1,  # Number of on-demand instances
            instance_types=None,
            spot_allocation_strategy='price-capacity-optimized',
            fleet_type='instant',
            subnet_ids=None
    ):
        """
        Launch an EC2 Fleet with a mix of on-demand and spot instances.
        
        Args:
            launch_template_id (str): ID of the launch template to use
            total_target_capacity (int): Total number of instances to launch
            on_demand_count (int): Number of on-demand instances (rest will be spot)
            instance_types (list): List of instance types to consider
            spot_allocation_strategy (str): Strategy for allocating spot instances
            fleet_type (str): Type of fleet ('instant', 'request', or 'maintain')
            subnet_ids (list): List of subnet IDs to launch instances in
            
        Returns:
            dict: Contains fleet response and detailed information about launched 
                  instances
        """
        ec2_client = boto3.client('ec2', region_name=self.region_name)
        
        # Calculate spot count
        spot_count = max(0, total_target_capacity - on_demand_count)
        
        # Create overrides for each instance type if specified
        launch_template_configs = []
        
        if instance_types:
            # Create overrides for each instance type
            overrides = []
            # for instance_type in instance_types:
            #     override = {'InstanceType': instance_type}
            #     if subnet_ids and len(subnet_ids) == 1:
            #         override['SubnetId'] = subnet_ids[0]
            #     overrides.append(override)
                
            launch_template_config = {
                'LaunchTemplateSpecification': {
                    'LaunchTemplateId': launch_template_id,
                    'Version': '$Latest'
                },
                'Overrides': [{
                    "SubnetId": subnet_ids[0] if subnet_ids else None,
                    "InstanceRequirements": {  # REQUIRED
                        "VCpuCount": { "Min": 1, "Max": 8 },
                        "MemoryMiB": { "Min": 2048 },
                        "AllowedInstanceTypes": instance_types
                    }
                }] + overrides
            }
        else:
            # No instance type overrides
            launch_template_config = {
                'LaunchTemplateSpecification': {
                    'LaunchTemplateId': launch_template_id,
                    'Version': '$Latest'
                },
                "Overrides": [{
                    "InstanceRequirements": {  # REQUIRED
                        "VCpuCount": { "Min": 2, "Max": 8 },
                        "MemoryMiB": { "Min": 2048 },
                    }
                }]
            }
            
            # Add subnet overrides if multiple subnets specified
            if subnet_ids and len(subnet_ids) > 1:
                overrides = []
                for subnet_id in subnet_ids:
                    overrides.append({'SubnetId': subnet_id})
                launch_template_config['Overrides'] += overrides
        
        launch_template_configs.append(launch_template_config)
        
        # Prepare fleet specifications
        fleet_specs = {
            'LaunchTemplateConfigs': launch_template_configs,
            'TargetCapacitySpecification': {
                'TargetCapacityUnitType': 'vcpu',
                'TotalTargetCapacity': total_target_capacity,
                'OnDemandTargetCapacity': on_demand_count,
                'SpotTargetCapacity': spot_count,
                # Default is spot, but we specify exact counts
                'DefaultTargetCapacityType': 'spot'  
            },
            'Type': fleet_type
        }
        
        # Add spot options
        if spot_count > 0:
            fleet_specs['SpotOptions'] = {
                'AllocationStrategy': spot_allocation_strategy,
                'InstanceInterruptionBehavior': 'terminate'
            }
        
        # Add on-demand options
        if on_demand_count > 0:
            fleet_specs['OnDemandOptions'] = {
                'AllocationStrategy': 'lowest-price'
            }
        
        try:
            # Create the fleet
            if fleet_type == "maintain":
                fleet_specs["ReplaceUnhealthyInstances"] = False
                fleet_specs["ExcessCapacityTerminationPolicy"] = 'no-termination'
            print(fleet_specs)
            fleet_response = ec2_client.create_fleet(**fleet_specs)
            fleet_id = fleet_response['FleetId']
            print(
                f"Successfully created EC2 Fleet {fleet_id} with "
                f"{on_demand_count} on-demand and {spot_count} spot instances"
            )

            print(fleet_response)
            
            # Get detailed information about the instances
            instances_info = {
                'on_demand_instances': [],
                'spot_instances': [],
                'all_instances': []
            }
            
            # For instant fleets, we need to use describe_fleets 
            # to get instance information
            if fleet_type == 'instant':
                time.sleep(5)
                fleet_details = ec2_client.describe_fleets(FleetIds=[fleet_id])
                print(fleet_details)
                
                if fleet_details['Fleets']:
                    fleet = fleet_details['Fleets'][0]
                    if 'Instances' in fleet:
                        # Get on-demand instances
                        for instances_data in fleet['Instances']:
                            if instances_data['Lifecycle'] == 'on-demand':
                                instance_ids = instances_data['InstanceIds']
                                for instance_id in instance_ids:
                                    instances_info['on_demand_instances'].append({
                                        'instance_id': instance_id,
                                        'instance_type': instances_data.get('InstanceType', 'N/A'),
                                        'lifecycle': 'on-demand'
                                    })
                                    instances_info['all_instances'].append(instance_id)
                            elif instances_data['Lifecycle'] == 'spot':
                                instance_ids = instances_data['InstanceIds']
                                for instance_id in instance_ids:
                                    instances_info['spot_instances'].append({
                                        'instance_id': instance_id,
                                        'instance_type': instances_data.get('InstanceType', 'N/A'),
                                        'lifecycle': 'spot',
                                    })
                                    instances_info['all_instances'].append(instance_id)

                print(instances_info['all_instances'])
            # For non-instant fleets, use describe_fleet_instances
            else:
                time.sleep(20)
                fleet_instances = ec2_client.describe_fleet_instances(
                    FleetId=fleet_id
                )
                print(fleet_instances)
                
                # We need to get instance details to determine 
                # if they're spot or on-demand
                instance_ids = [
                    instance['InstanceId'] 
                    for instance in fleet_instances['ActiveInstances']
                ]
                print(instance_ids)
                
                if instance_ids:
                    instances_response = ec2_client.describe_instances(
                        InstanceIds=instance_ids
                    )
                    
                    for reservation in instances_response['Reservations']:
                        for instance in reservation['Instances']:
                            instance_id = instance['InstanceId']
                            instance_type = instance['InstanceType']
                            
                            # Check if it's a spot instance
                            if ('InstanceLifecycle' in instance and 
                                    instance['InstanceLifecycle'] == 'spot'):
                                instances_info['spot_instances'].append({
                                    'instance_id': instance_id,
                                    'instance_type': instance_type,
                                    'lifecycle': 'spot',
                                    'spot_instance_request_id': instance.get(
                                        'SpotInstanceRequestId', 'N/A')
                                })
                            else:
                                instances_info['on_demand_instances'].append({
                                    'instance_id': instance_id,
                                    'instance_type': instance_type,
                                    'lifecycle': 'on-demand'
                                })
                            
                            instances_info['all_instances'].append(instance_id)
            
            # Wait for instances to be in the running state
            if instances_info['all_instances']:
                waiter = ec2_client.get_waiter('instance_running')
                try:
                    waiter.wait(InstanceIds=instances_info['all_instances'])
                    print(
                        f"All {len(instances_info['all_instances'])} "
                        f"instances are now running"
                    )
                except Exception as e:
                    print(f"Warning: Not all instances reached running state: {e}")
            
            # Get detailed information about all instances
            if instances_info['all_instances']:
                detailed_instances = ec2_client.describe_instances(
                    InstanceIds=instances_info['all_instances']
                )

                #print(detailed_instances)
                
                # Process and add detailed information
                detailed_info = []
                unique_instance_types = []
                for reservation in detailed_instances['Reservations']:
                    for instance in reservation['Instances']:
                        # Determine if it's spot or on-demand
                        lifecycle = 'on-demand'
                        if ('InstanceLifecycle' in instance and 
                                instance['InstanceLifecycle'] == 'spot'):
                            lifecycle = 'spot'
                        
                        # Get instance name if it exists
                        instance_name = 'N/A'
                        if 'Tags' in instance:
                            for tag in instance['Tags']:
                                if tag['Key'] == 'Name':
                                    instance_name = tag['Value']
                                    break
                        
                        # Get private and public IPs
                        private_ip = instance.get('PrivateIpAddress', 'N/A')
                        public_ip = instance.get('PublicIpAddress', 'N/A')
                        
                        detailed_info.append({
                            'instance_id': instance['InstanceId'],
                            'instance_type': instance['InstanceType'],
                            'lifecycle': lifecycle,
                            'state': instance['State']['Name'],
                            'name': instance_name,
                            'private_ip': private_ip,
                            'public_ip': public_ip,
                            'public_dns': instance.get('PublicDnsName', 'N/A'),
                            'private_dns': instance.get('PrivateDnsName', 'N/A'),
                            'availability_zone': instance['Placement']['AvailabilityZone'],
                            'subnet_id': instance.get('SubnetId', 'N/A'),
                            'launch_time': instance['LaunchTime'].isoformat()
                        })

                        instance_type = instance['InstanceType']
                        if instance_type not in unique_instance_types:
                            unique_instance_types.append(instance_type)
                
                instances_info['detailed_instances'] = detailed_info
            
            vcpus_map = self.get_vcpus(unique_instance_types)
            for instance in instances_info['detailed_instances']:
                instance['vcpus'] = vcpus_map.get(instance['instance_type'], -1)

            self.active_instances = instances_info['detailed_instances']  # Save active instances

            return fleet_id
        
        except Exception as e:
            print(f"Error creating EC2 Fleet: {e}")
            raise
    
    def get_vcpus(self, instance_types):
        """
        Get the number of vCPUs for the specified instance types.
        
        Args:
            instance_types (list): List of EC2 instance types
            
        Returns:
            dict: Mapping of instance type to vCPU count
        """
        ec2 = boto3.client('ec2', region_name=self.region_name)

        # Prepare the request parameters
        params = {}
        if instance_types:
            params['InstanceTypes'] = instance_types
        
        # Get instance type information
        instance_vcpus = {}
        paginator = ec2.get_paginator('describe_instance_types')
        
        for page in paginator.paginate(**params):
            for instance_type in page['InstanceTypes']:
                type_name = instance_type['InstanceType']
                vcpu_count = instance_type['VCpuInfo']['DefaultVCpus']
                instance_vcpus[type_name] = vcpu_count
        
        return instance_vcpus
    
    async def write_nodelist_file(self, filename):
        """
        Write a nodelist file for Charm++ to an instance.
        
        Args:
            filename (str): Path where the nodelist file should be created
            
        Returns:
            str: Instance ID of the master node
        """
        nodelist_str = ''
        updated_instances = []
        for instance in self.active_instances:
            if instance['lifecycle'] == 'on-demand':
                master = instance
                nodelist_str = (
                    f"host {instance['private_dns']} ++cpus {instance['vcpus']}\n" 
                    + nodelist_str
                )
                updated_instances = [instance] + updated_instances  # Master first
            else:
                nodelist_str += f"host {instance['private_dns']} ++cpus {instance['vcpus']}\n"
                updated_instances.append(instance)

        self.active_instances = updated_instances
        print("Writing nodelist file: ", nodelist_str)

        escaped_content = nodelist_str.replace("'", "'\\''")
        command = f"echo '{escaped_content}' > {filename}"

        await self.run_command(command, master['public_dns'])
        return master
    
    async def update_nodelist_file(self, interrupted_instances, new_instances, filename):
        # FIXME for now ignoring new_instances
        #new_active_instances = []
        nodelist_str = ""
        for instance in self.active_instances:
            if not instance['instance_id'] in interrupted_instances:
                #new_active_instances.append(instance)
                nodelist_str += f"host {instance['private_dns']} ++cpus {instance['vcpus']}\n"

        for instance in new_instances:
            nodelist_str += f"host {instance['private_dns']} ++cpus {instance['vcpus']}\n"

        #self.active_instances = new_active_instances
        master = self.active_instances[0]
        
        print("Writing nodelist file: ", nodelist_str)

        escaped_content = nodelist_str.replace("'", "'\\''")
        command = f"> {filename} && echo '{escaped_content}' > {filename}"

        await self.run_command(command, master['public_dns'])
        return master
        
    async def run_command(self, command, public_ip, output_file=None, capture_output=False):
        """
        Run a command on an EC2 instance and save/capture the output asynchronously.
        
        Args:
            command (str): Command to run
            public_ip (str): Public IP or DNS of the instance
            output_file (str, optional): Base name for output files (.out and .err will be appended)
            capture_output (bool, optional): Whether to capture and return output (default: False)
        
        Returns:
            dict: Contains exit_status and captured stdout/stderr if capture_output=True
        """
        # Prepare the output files
        if output_file:
            fout = open(output_file + ".out", 'w')
            ferr = open(output_file + ".err", 'w')
        else:
            fout = sys.stdout
            ferr = sys.stderr
        
        # For output capturing
        stdout_content = []
        stderr_content = []
        
        try:
            # Connect to the SSH server
            async with asyncssh.connect(
                host=public_ip,
                username='ec2-user',
                client_keys=[self.key_path],
                known_hosts=None  # Equivalent to AutoAddPolicy
            ) as conn:
                # Run the command
                async with conn.create_process(command) as process:
                    # Process stdout and stderr concurrently
                    async def handle_stdout():
                        async for line in process.stdout:
                            fout.write(line)
                            fout.flush()
                            if capture_output:
                                stdout_content.append(line)
                    
                    async def handle_stderr():
                        async for line in process.stderr:
                            ferr.write(line)
                            ferr.flush()
                            if capture_output:
                                stderr_content.append(line)
                    
                    # Create tasks for stdout and stderr
                    stdout_task = asyncio.create_task(handle_stdout())
                    stderr_task = asyncio.create_task(handle_stderr())
                    
                    # Wait for both tasks to complete
                    await asyncio.gather(stdout_task, stderr_task)
                    
                    # Get the exit status
                    exit_status = await process.wait()
                    print(f"\nCommand exited with status {exit_status}")
                    
                    if capture_output:
                        return {
                            'exit_status': exit_status,
                            'stdout': ''.join(stdout_content),
                            'stderr': ''.join(stderr_content)
                        }
                    else:
                        return exit_status
        
        except (OSError, asyncssh.Error) as exc:
            error_msg = f"SSH connection failed in {command}: {exc}"
            print(error_msg, file=sys.stderr)
            if output_file:
                ferr.write(error_msg + "\n")
            
            if capture_output:
                return {
                    'exit_status': 255,
                    'stdout': '',
                    'stderr': error_msg
                }
            else:
                return 255
        
        finally:
            # Close file handles if needed
            if output_file:
                fout.close()
                ferr.close()

    async def check_interruptions(self, instances, interrupted_instances):
        """
        Monitor EC2 instances for spot interruption notices using CloudWatch Events.
        
        Args:
            instance_ids (list): List of instance IDs to monitor.
            window_seconds (int, optional): Time window in seconds to check for interruption notices.
                                        Default is 60 seconds (1 minute).
        
        Returns:
            dict: Dictionary of instances with interruption notices and their details
        """
        # Validate input
        if not instances:
            raise ValueError("At least one instance ID must be provided")
        
        check_command = (
            "TOKEN=`curl -X PUT \"http://169.254.169.254/latest/api/token\" "
            "-H \"X-aws-ec2-metadata-token-ttl-seconds: 21600\"` && "
            "curl -S http://169.254.169.254/latest/meta-data/spot/instance-action "
            "-H \"X-aws-ec2-metadata-token: $TOKEN\""
        )

        num_interruptions = 0
        for instance in instances:
            if instance['lifecycle'] == 'spot':
                # Check for interruption notice using instance metadata
                try:
                    print("Checking instance: ", instance['instance_id'], 
                          instance['public_dns'])
                    result = await self.run_command(check_command, instance['public_dns'],
                                                    capture_output=True)
                    output = result['stdout'] if isinstance(result, dict) else result
                    if "\"action\":\"terminate\"" in output:
                        print(f"Instance {instance['instance_id']} has a spot interruption notice.")
                        interrupted_instances.append(instance['instance_id'])
                        num_interruptions += 1
                except Exception as e:
                    print(f"Error checking instance {instance['instance_id']}: {str(e)}")
        
        return num_interruptions, interrupted_instances
    
    def check_replacement_instances(self, instance_ids, fleet_id):
        ec2_client = boto3.client('ec2', region_name=self.region_name)
        try:
            # Get all instances in the fleet
            fleet_instances = ec2_client.describe_fleet_instances(FleetId=fleet_id)
            active_instance_ids = [
                instance['InstanceId'] for instance in fleet_instances['ActiveInstances']
            ]

            # Find instances that are not in the provided instance_ids
            new_instances = [
                instance_id for instance_id in active_instance_ids \
                    if instance_id not in instance_ids and instance_id not in self.interrupted_instances
            ]

            # Get detailed information about the new instances
            if new_instances:
                instances_response = ec2_client.describe_instances(InstanceIds=new_instances)
                detailed_instances = []
                for reservation in instances_response['Reservations']:
                    for instance in reservation['Instances']:
                        instance_info = {
                            'instance_id': instance['InstanceId'],
                            'instance_type': instance['InstanceType'],
                            'lifecycle': 'spot' if 'InstanceLifecycle' in instance and instance['InstanceLifecycle'] == 'spot' else 'on-demand',
                            'private_dns': instance.get('PrivateDnsName', 'N/A'),
                            'public_dns': instance.get('PublicDnsName', 'N/A'),
                            'private_ip': instance.get('PrivateIpAddress', 'N/A'),
                            'public_ip': instance.get('PublicIpAddress', 'N/A'),
                            'vcpus': self.get_vcpus([instance['InstanceType']]).get(instance['InstanceType'], -1)
                        }
                        detailed_instances.append(instance_info)
                waiter = ec2_client.get_waiter('instance_running')
                try:
                    waiter.wait(InstanceIds=new_instances)
                except Exception as e:
                    print(f"Warning: Not all instances reached running state: {e}")
                time.sleep(10)
                return detailed_instances
            else:
                return []
        except Exception as e:
            print(f"Error checking replacement instances: {e}")
            raise
    
    def find_killed_pes(self, interrupted_instances):
        killed = []
        current_pes = 0
        for i, instance in enumerate(self.active_instances):
            if instance['instance_id'] in interrupted_instances:
                for j in range(current_pes, current_pes + instance['vcpus']):
                    killed.append(j)
            current_pes += instance['vcpus']
        return killed

    async def send_signal(self, master, interrupted_instances, new_instances):
        client = "/home/ec2-user/charm/examples/charm++/shrink_expand/client"
        killed_pes = self.find_killed_pes(interrupted_instances)
        rescale_command = f"{client} {master['private_ip']} 1234 {sum([i['vcpus'] for i in self.active_instances])} " \
                          f"{len(killed_pes)} {' '.join(map(str, killed_pes))} " \
                          f"{sum([i['vcpus'] for i in new_instances])}"
        await self.run_command(rescale_command, master['public_dns'])

        new_active_instances = []
        for i, instance in enumerate(self.active_instances):
            if instance['instance_id'] not in interrupted_instances:
                new_active_instances.append(instance)
        self.active_instances = new_active_instances + new_instances
    
    async def monitor_instances(self, fleet_id, timeout=10, setup_command=None):
        while True:
            instance_ids = [instance['instance_id'] for instance in self.active_instances]
            num_interruptions, self.interrupted_instances = await self.check_interruptions(
                self.active_instances, self.interrupted_instances)
            new_instances = self.check_replacement_instances(instance_ids, fleet_id)

            # send signal to application to shrink/expand
            if num_interruptions > 0 or len(new_instances) > 0:
                print("ACTIVE INSTANCES: ", len(self.active_instances))

                if setup_command and new_instances:
                    setup_tasks = []
                    for instance in new_instances:
                        setup_tasks.append(
                            asyncio.create_task(self.run_command(setup_command, instance['public_dns']))
                        )
                    await asyncio.gather(*setup_tasks)

                master = await self.update_nodelist_file(
                    self.interrupted_instances, new_instances, "/tmp/nodelist"
                    )
                print("Sending signal to master: ", master['private_ip'])
                await self.send_signal(master, self.interrupted_instances, new_instances)

            await asyncio.sleep(timeout)
    
    async def async_run(
            self,
            ami_id,
            instance_types,
            cluster_name,
            command,
            setup_command=None,
            total_target_capacity=3,
            on_demand_count=1,  # One on-demand, rest spot
            key_name=None,
            security_group_ids=None,
            subnet_ids=None,
            user_data=None,
            output_file='output'
    ):
        """
        Create a clustered EC2 fleet with a mix of on-demand and spot instances.
        
        Args:
            ami_id (str): AMI ID to use
            instance_types (list): List of instance types to consider
            cluster_name (str): Base name for the cluster resources
            command (str): Command to run on the master node
            total_target_capacity (int): Total number of instances to launch
            on_demand_count (int): Number of on-demand instances (rest will be spot)
            key_name (str): SSH key pair name
            security_group_ids (list): List of security group IDs
            subnet_ids (list): List of subnet IDs
            user_data (str): User data script
            output_file (str): Path to the file where command output will be saved
        """
        # Create a placement group
        placement_group_name = f"{cluster_name}-pg-cluster"
        self.create_placement_group(placement_group_name, strategy='cluster')
        
        # Create a launch template with the placement group
        template_name = f"{cluster_name}-template-cluster"
        template_id = self.create_launch_template(
            template_name=template_name,
            ami_id=ami_id,
            instance_type=instance_types[0],  # Use first instance type as default
            key_name=key_name,
            security_group_ids=security_group_ids,
            placement_group=placement_group_name,
            user_data=user_data,
            tags=[
                {
                    'Key': 'Name',
                    'Value': f"{cluster_name}-instance"
                },
                {
                    'Key': 'Cluster',
                    'Value': cluster_name
                }
            ]
        )
        
        # Launch the EC2 fleet with mixed instance types
        fleet_id = self.launch(
            launch_template_id=template_id,
            total_target_capacity=total_target_capacity,
            on_demand_count=on_demand_count,
            instance_types=instance_types,
            spot_allocation_strategy='price-capacity-optimized',
            fleet_type='maintain',
            subnet_ids=subnet_ids
        )

        time.sleep(10)

        master = await self.write_nodelist_file('/tmp/nodelist')

        num_pes = sum([i['vcpus'] for i in self.active_instances])
        command = command % {'num_pes': num_pes}

        # Run setup_command on all active instances if provided
        if setup_command:
            setup_tasks = []
            for instance in self.active_instances:
                setup_tasks.append(
                    asyncio.create_task(self.run_command(setup_command, instance['public_dns']))
                )
            await asyncio.gather(*setup_tasks)

        # run the charmrun command on master node
        run_task = asyncio.create_task(self.run_command(command, master['public_dns'], output_file=output_file))
        monitor_task = asyncio.create_task(
            self.monitor_instances(
                fleet_id,
                timeout=10  # Poll every 10 seconds
            )
        )

        # Wait for the first task to complete
        await run_task

        print(f"Cancelling pending task...")
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            print("Task was cancelled successfully")
        
        self.terminate_fleet(fleet_id, terminate_instances=True)

    def run(
            self,
            ami_id,
            instance_types,
            cluster_name,
            command,
            setup_command=None,
            total_target_capacity=3,
            on_demand_count=1,  # One on-demand, rest spot
            key_name=None,
            security_group_ids=None,
            subnet_ids=None,
            user_data=None,
            output_file='output'
    ):
        asyncio.run(self.async_run(
            ami_id=ami_id,
            instance_types=instance_types,
            cluster_name=cluster_name,
            command=command,
            setup_command=setup_command,
            total_target_capacity=total_target_capacity,
            on_demand_count=on_demand_count,
            key_name=key_name,
            security_group_ids=security_group_ids,
            subnet_ids=subnet_ids,
            user_data=user_data,
            output_file=output_file
        ))

    def terminate_fleet(self, fleet_id, terminate_instances=True):
        """
        Terminate an EC2 Fleet.
        
        Args:
            fleet_id (str): The ID of the EC2 Fleet to terminate
            terminate_instances (bool): Whether to terminate the instances in the fleet
                                    If False, the instances will continue running
        
        Returns:
            dict: Response from the delete_fleets API call containing termination details
        """
        ec2_client = boto3.client('ec2', region_name=self.region_name)
        
        try:
            # Determine the termination behavior
            termination_behavior = 'terminate' if terminate_instances else 'no-termination'
            
            # Delete the fleet
            response = ec2_client.delete_fleets(
                FleetIds=[fleet_id],
                TerminateInstances=terminate_instances
            )
            
            # Check if the deletion was successful
            successful_deletions = response.get('SuccessfulFleetDeletions', [])
            unsuccessful_deletions = response.get('UnsuccessfulFleetDeletions', [])
            
            if successful_deletions:
                fleet_info = successful_deletions[0]
                print(
                    f"Successfully deleted fleet {fleet_id} "
                    f"with termination behavior: {termination_behavior}"
                )
                
                if terminate_instances:
                    print("Instances are being terminated")
                else:
                    print("Instances will continue running")
            
            if unsuccessful_deletions:
                for failure in unsuccessful_deletions:
                    error_code = failure.get('Error', {}).get('Code', 'Unknown')
                    error_message = failure.get('Error', {}).get('Message', 'Unknown error')
                    print(
                        f"Failed to delete fleet {failure.get('FleetId')}: "
                        f"{error_code} - {error_message}"
                    )
            
            return response
        
        except Exception as e:
            print(f"Error terminating EC2 Fleet {fleet_id}: {e}")
            raise
