import aws_cdk as core
import aws_cdk.assertions as assertions

from reconcileai no sap cdk.reconcileai no sap cdk_stack import ReconcileaiNoSapCdkStack

# example tests. To run these tests, uncomment this file along with the example
# resource in reconcileai no sap cdk/reconcileai no sap cdk_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = ReconcileaiNoSapCdkStack(app, "reconcileai-no-sap-cdk")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
