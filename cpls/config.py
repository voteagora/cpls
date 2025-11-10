
import os
from dotenv import load_dotenv
from pathlib import Path
import yaml

load_dotenv()

ENVIRONMENT = os.getenv("ENVIRONMENT", "dev") # or prod

# Load configuration from environment
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "cpls-usmr-dev-25q3")
SERVER_HOST = os.getenv("SERVER_HOST", "0.0.0.0")
SERVER_PORT = int(os.getenv("SERVER_PORT", "8001"))
SCHEDULER_INTERVAL_MINUTES = int(os.getenv("SCHEDULER_INTERVAL_MINUTES", "10"))
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY", '')
BLOCKCACHE_URL = os.getenv("BLOCKCACHE_URL", 'https://blockcache-production.up.railway.app/')
DATABASE_URL = os.getenv("DATABASE_URL")
print(DATABASE_URL)
WRITE_TO_DISK = ENVIRONMENT == 'dev'
TENANTS_CONFIG_PATH = Path(os.getenv("TENANT_CONFIG_PATH", '/config/envs/prod'))
DEPLOYMENT = os.getenv("DEPLOYMENT", "main")
INFRA_DAO_SLUGS = os.getenv("INFRA_DAO_SLUGS", "optimism,cyber,pguild,syndicate")
INFRA_DAO_SLUGS = INFRA_DAO_SLUGS.split(',')

RESET_PROPOSALS_ON_RESTART = os.getenv("RESET_PROPOSALS_ON_RESTART", "true").lower() == "true"


def load_tenant_configs():
    """
    Load all YAML tenant configuration files from TENANTS_CONFIG_PATH.
    Returns a dictionary mapping tenant slug (filename without .yaml) to config data.
    """
    tenant_configs = {}

    if not TENANTS_CONFIG_PATH.exists():
        print(f"Warning: Tenant config path does not exist: {TENANTS_CONFIG_PATH}")
        return tenant_configs

    for yaml_file in TENANTS_CONFIG_PATH.glob("*.yaml"):
        try:
            with open(yaml_file, 'r') as f:
                config_data = yaml.safe_load(f)

                if DEPLOYMENT not in config_data['deployments']:
                    print(f"Warning: Deployment {DEPLOYMENT} not found in {yaml_file}, skipping.")
                    continue

                deployment = config_data['deployments'][DEPLOYMENT]
                del config_data['deployments']
                config_data['deployment'] = deployment

                # Use filename without extension as the key (e.g., 'optimism', 'scroll')
                tenant_slug = yaml_file.stem
                tenant_configs[tenant_slug] = config_data
                print(f"Loaded tenant config: {tenant_slug}")
        except Exception as e:
            print(f"Error loading {yaml_file}: {e}")

    return tenant_configs

# TODO refactor this to invert it.  We we should be calling this method
def load_tenant_config(dao_infra_slug):
    configs = load_tenant_configs()
    return configs[dao_infra_slug]