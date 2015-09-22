import boto3


def get_subnet_ids_from_vpc_stack(region):
    cfn = boto3.client('cloudformation', region_name=region)
    result = cfn.describe_stacks(StackName='vpc')

    vpc_stack = result["Stacks"][0]
    outputs = vpc_stack["Outputs"]

    for output in outputs:
        if output["OutputKey"] == "subnets":
            return output["OutputValue"]

    raise Exception("No subnets output found for vpc stack")


def get_first_subnet_id_from_vpc_stack(region):
    subnets = get_subnet_ids_from_vpc_stack(region)
    return subnets.split(',')[0]
