import pathlib

import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent


def test_all_yaml_parses_and_referenced_files_exist():
    for f in [ROOT / "databricks.yml", *(ROOT / "resources").glob("*.yml")]:
        yaml.safe_load(f.read_text())

    job = yaml.safe_load((ROOT / "resources" / "spotify_azure.job.yml").read_text())
    for task in job["resources"]["jobs"]["spotify_azure_job"]["tasks"]:
        nb = task.get("notebook_task")
        if nb:
            assert (ROOT / "resources" / nb["notebook_path"]).resolve().exists(), nb

    pipe = yaml.safe_load((ROOT / "resources" / "spotify_azure_etl.pipeline.yml").read_text())
    for lib in pipe["resources"]["pipelines"]["spotify_azure_etl"]["libraries"]:
        assert (ROOT / "resources" / lib["glob"]["include"]).resolve().exists()
