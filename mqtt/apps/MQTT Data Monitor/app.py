from flask import Flask, request, jsonify, render_template
import logging
import pandas
from databricks import sql
from databricks.sdk.core import Config
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
import time
import paho.mqtt.client as mqtt_client

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Create logger for this application
logger = logging.getLogger(__name__)

# Suppress werkzeug logging to reduce noise
werkzeug_log = logging.getLogger('werkzeug')
werkzeug_log.setLevel(logging.ERROR)

flask_app = Flask(__name__)


client = WorkspaceClient()


# Database connection function
def get_data(query, warehouse_id, params=None):
    """Execute query with fallback to demo data"""
    try:
        cfg = Config()
        if warehouse_id and cfg.host:
            with sql.connect(
                server_hostname=cfg.host,
                http_path=f"/sql/1.0/warehouses/{warehouse_id}",
                credentials_provider=lambda: cfg.authenticate,
            ) as connection:
                if params:
                    df = pandas.read_sql(query, connection, params=params)
                else:
                    df = pandas.read_sql(query, connection)

                return df.to_dict('records')
    except Exception as e:
        logger.error(f"Database query failed: {e}")
        # Return empty list on error so we can show a message
        return []


def create_job(client, notebook_path, cluster_id):

    created_job = client.jobs.create(
        name=f"mqtt_{time.time_ns()}",
        tasks=[
            jobs.Task(
                description="mqtt",
                notebook_task=jobs.NotebookTask(notebook_path=notebook_path, source=jobs.Source("WORKSPACE")),
                task_key="mqtt",
                timeout_seconds=0,
                existing_cluster_id=cluster_id,
            )
        ],
    )
    return created_job


def run_job(client, created_job, mqtt_config):
    """Run the job with user-provided MQTT configuration"""
    run = client.jobs.run_now(
        job_id=created_job.job_id,
        notebook_params={
            "catalog": mqtt_config.get('catalog', 'dbdemos'),
            "database": mqtt_config.get('schema', 'dbdemos_mqtt'),
            "table": mqtt_config.get('table', 'mqtt_v5'),
            "broker": mqtt_config.get('broker_address', ''),
            "port": mqtt_config.get('port', '8883'),
            "username": mqtt_config.get('username', ''),
            "password": mqtt_config.get('password', ''),
            "topic": mqtt_config.get('topic', '#'),
            "qos": mqtt_config.get('qos', '0'),
            "require_tls": mqtt_config.get('require_tls', 'false'),
            "keepalive": mqtt_config.get('keepalive', '60'),
        },
    )
    return run


def mqtt_remote_client(mqtt_server_config):
    """Test MQTT connection with provided configuration"""
    connection_result = {'success': False, 'message': '', 'error': None}
    try:
        # Callback function for when the client connects
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                logger.info("Connected successfully to MQTT Broker!")
            else:
                logger.error(f"Failed to connect, return code {rc}")

        client = mqtt_client.Client(
            mqtt_client.CallbackAPIVersion.VERSION1, client_id="mqtt_connection_test", clean_session=True
        )

        # Set username and password if provided
        if mqtt_server_config.get("username") and mqtt_server_config.get("password"):
            client.username_pw_set(username=mqtt_server_config["username"], password=mqtt_server_config["password"])

        # Set TLS if required
        if mqtt_server_config.get("require_tls") == 'true':
            import ssl

            tls_config = {}
            if mqtt_server_config.get("ca_certs"):
                tls_config['ca_certs'] = mqtt_server_config["ca_certs"]
            if mqtt_server_config.get("certfile"):
                tls_config['certfile'] = mqtt_server_config["certfile"]
            if mqtt_server_config.get("keyfile"):
                tls_config['keyfile'] = mqtt_server_config["keyfile"]

            # If disable certs verification is checked
            if mqtt_server_config.get("tls_disable_certs") == 'true':
                tls_config['cert_reqs'] = ssl.CERT_NONE

            client.tls_set(**tls_config)

        client.on_connect = on_connect

        # Attempt connection
        port = int(mqtt_server_config.get("port", 8883))
        keepalive = int(mqtt_server_config.get("keepalive", 60))

        client.connect(mqtt_server_config["host"], port, keepalive)

        # Start the loop to process network traffic
        client.loop_start()

        # Give it a moment to connect
        time.sleep(2)

        # Check if connected
        if client.is_connected():
            connection_result['success'] = True
            connection_result['message'] = (
                f'Successfully connected to MQTT broker at {mqtt_server_config["host"]}:{port}'
            )
        else:
            connection_result['message'] = 'Failed to connect to MQTT broker'
            connection_result['error'] = (
                f'Failed to connect to {mqtt_server_config["host"]} and {port} and {mqtt_server_config["username"]} are not working'
            )

        # Disconnect
        client.loop_stop()
        client.disconnect()

    except Exception as e:
        connection_result['success'] = False
        connection_result['message'] = 'Connection failed'
        connection_result['error'] = str(e)

    return connection_result


# Initialize with empty data - will be loaded when user specifies catalog/schema/table and clicks refresh
curr_data = []


def get_mqtt_stats():
    """Get MQTT message statistics from data"""
    if not curr_data or len(curr_data) == 0:
        return {'duplicated_messages': 0, 'qos2_messages': 0, 'unique_topics': 0, 'total_messages': 0}

    # Count duplicated messages
    duplicated = len([a for a in curr_data if str(a.get('is_duplicate', 'false')).lower() == 'true'])
    # Count QoS 2 messages
    qos2_messages = len([a for a in curr_data if str(a.get('qos', 0)) == '2'])
    # Count unique topics
    unique_topics = len(set([str(a.get('topic', '')) for a in curr_data]))
    # Total row count
    row_count = len(curr_data)

    return {
        'duplicated_messages': duplicated,
        'qos2_messages': qos2_messages,
        'unique_topics': unique_topics,
        'total_messages': row_count,
    }


@flask_app.route('/api/test-mqtt-connection', methods=['POST'])
def test_mqtt_connection():
    """API endpoint to test MQTT broker connection"""
    try:
        # Get the configuration from the request
        mqtt_config = request.json

        # Validate required fields
        if not mqtt_config.get('broker_address'):
            return jsonify({'success': False, 'error': 'Broker address is required'}), 400

        # Prepare config for MQTT test
        mqtt_server_config = {
            'host': mqtt_config.get('broker_address'),
            'port': mqtt_config.get('port', '1883'),
            'username': mqtt_config.get('username', ''),
            'password': mqtt_config.get('password', ''),
            'require_tls': mqtt_config.get('require_tls', 'false'),
            'ca_certs': mqtt_config.get('ca_certs', ''),
            'certfile': mqtt_config.get('certfile', ''),
            'keyfile': mqtt_config.get('keyfile', ''),
            'tls_disable_certs': mqtt_config.get('tls_disable_certs', 'false'),
            'keepalive': mqtt_config.get('keepalive', '60'),
        }
        # Test the connection
        result = mqtt_remote_client(mqtt_server_config)

        if result['success']:
            return jsonify(result), 200
        else:
            return jsonify(result), 400

    except Exception as e:
        return jsonify({'success': False, 'message': 'Connection test failed', 'error': str(e)}), 500


@flask_app.route('/api/start-mqtt-job', methods=['POST'])
def start_mqtt_job():
    """API endpoint to start the MQTT data ingestion job with user configuration"""
    try:
        # Get the configuration from the request
        mqtt_config = request.json

        # Validate required fields
        if not mqtt_config.get('broker_address'):
            return jsonify({'success': False, 'error': 'Broker address is required'}), 400

        if not mqtt_config.get('catalog') or not mqtt_config.get('schema') or not mqtt_config.get('table'):
            return jsonify({'success': False, 'error': 'Catalog, Schema, and Table name are required'}), 400

        # Get notebook_path and cluster_id from config
        notebook_path = mqtt_config.get('notebook_path')
        cluster_id = mqtt_config.get('cluster_id')

        # Create the job
        created_job = create_job(client, notebook_path, cluster_id)

        # Run the job with user configuration
        run = run_job(client, created_job, mqtt_config)

        catalog = mqtt_config.get('catalog')
        schema = mqtt_config.get('schema')
        table = mqtt_config.get('table')

        return jsonify(
            {
                'success': True,
                'job_id': created_job.job_id,
                'run_id': run.run_id,
                'message': f'MQTT data ingestion job started successfully. Data will be written to {catalog}.{schema}.{table}',
            }
        )

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/refresh-data', methods=['POST'])
def refresh_data():
    """API endpoint to refresh dashboard data from specified table"""
    global curr_data
    try:
        data = request.json
        catalog = data.get('catalog')
        schema = data.get('schema')
        table = data.get('table')
        warehouse_id = data.get('warehouse_id', '4b9b953939869799')  # Default fallback

        # Validate required fields
        if not catalog or not schema or not table:
            return jsonify({'success': False, 'error': 'Catalog, Schema, and Table name are required'}), 400

        # Build the query with parameterized values
        query = (
            "SELECT message, is_duplicate, qos, topic, received_time FROM %s.%s.%s ORDER BY received_time DESC LIMIT %s"
        )

        # Fetch data using get_data function with parameters
        curr_data = get_data(query, warehouse_id, (catalog, schema, table, 100))

        # Calculate stats from refreshed data
        stats = get_mqtt_stats()

        return jsonify(
            {
                'success': True,
                'message': f'Data refreshed from {catalog}.{schema}.{table}',
                'row_count': len(curr_data) if curr_data else 0,
                'data': curr_data,  # Return the actual data to update the UI
                'stats': stats,  # Return the calculated stats
            }
        )
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/')
def dashboard():
    """Main MQTT Data Monitor dashboard page"""
    stats = get_mqtt_stats()

    return render_template('dashboard.html', stats=stats, curr_data=curr_data)


def get_alert_icon(alert_type):
    if 'IoT Sensor' in alert_type:
        return 'fa-thermometer-half'
    elif 'Device Status' in alert_type:
        return 'fa-wifi'
    elif 'System Metrics' in alert_type:
        return 'fa-chart-bar'
    elif 'Security' in alert_type:
        return 'fa-shield-alt'
    else:
        return 'fa-broadcast-tower'


def get_status_color(status):
    if status == 'new':
        return 'bg-blue-100 text-blue-800'
    elif status == 'in_progress':
        return 'bg-yellow-100 text-yellow-800'
    elif status == 'resolved':
        return 'bg-green-100 text-green-800'
    else:
        return 'bg-gray-100 text-gray-800'


def get_severity_color(severity):
    if severity == 'CRITICAL':
        return 'bg-red-100 text-red-800'
    elif severity == 'WARNING':
        return 'bg-yellow-100 text-yellow-800'
    elif severity == 'INFO':
        return 'bg-blue-100 text-blue-800'
    else:
        return 'bg-gray-100 text-gray-800'


if __name__ == '__main__':
    logger.info("Starting MQTT Data Monitor Dashboard")
    logger.info("MQTT Message Processing & Analytics Platform")
    logger.info("=" * 50)
    flask_app.run(debug=True, host='0.0.0.0', port=8001)
