Welcome to your new dbt project!

### Setting up Develop Environment

Requirements:
- pyenv

To setup you Develop Environment you need to create a `.env` file with the follow content:


```python
SANDBOX_HOST=
SANDBOX_USER=
SANDBOX_PWD=
SANDBOX_DB=
SANDBOX_SCHEMA=
PROD_HOST=
PROD_USER=
PROD_PWD=
PROD_DB=
PROD_SCHEMA=
DBT_USER=
DBT_PROFILES_DIR=
DBT_RUNS_LOCALLY="True"
```

Where you should include your credentials for each environment and there is three especial environment variables:

`DBT_USER`: should contain your user this would be used as prefix for the creation of your develop schema.

`DBT_PROFILES_DIR:` should be empty value, this would look for the profile files in the root directory of this dbt project.

`DBT_RUNS_LOCALLY:` should be set it as `True` to all models goes created on your new develop environment, if you want to run it directly to the `public` schema this variable should set it on `False` or not been presented.

Once all variables are set we could start our virtual environment running the next command:
```bash
pyenv shell
```

and then we could start running dbt command from that virtual environment without trouble.

### Directory structure:

* All models are stored inside the models directory and the directory structure it follows is

```text
.
├── models
│   ├── base
│   │    ├──brokerage
│   │    │     ├── brokerage_assignments.sql
│   │    │     ├── brokerages.sql
│   │    │     └── ...
│   │    ├──lead
│   │    │     ├── leads.sql
│   │    │     ├── leads_data.sql
│   │    │     └── ...
│   │    └── ...
│   ├── staging
│   │    ├──lead
│   │    │     ├── stg_lead_banks.sql
│   │    │     ├── stg_lead_current_lead_statuses_agg.sql
│   │    │     └── ...
│   │    └── ...
│   ├── marts
│   │    └── lead
│   │    │     ├── lead_data_v3.sql
│   │    └── ...
├── ...
```

**Base** — creating our atoms, our initial modular building blocks, from source data

**Staging** — stacking layers of logic with clear and specific purposes to prepare our staging models to join into the entities we want

**Marts** — bringing together our modular pieces into a wide, rich vision of the entities our organization cares about

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Fundamentals [course](https://courses.getdbt.com/courses/fundamentals) from dbt
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
