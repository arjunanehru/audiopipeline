import audiopipeline

def lambda_handler(event, context):
    try:
        audiopipeline.main()
        return {
            'statusCode': 200,
            'body': 'Execution successful'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }
