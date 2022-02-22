import typer

from DataManager.config_files.set_config_file import set_keys
import os
app = typer.Typer()
set_app = typer.Typer()
app.add_typer(set_app, name="set")

@set_app.command()
def api_keys(section: str, public_key: str, private_key: str):
    s = set_keys(section, public_key, private_key)
    typer.echo(os.getcwd())

def main():
    app()