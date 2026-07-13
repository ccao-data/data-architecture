def model(dbt, session):
    prod = dbt.source("model", "metadata")
    dev = dbt.source("z_dev_model", "metadata")
    return prod.unionByName(dev, allowMissingColumns=True)
