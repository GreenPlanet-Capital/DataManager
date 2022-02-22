import typer

import DataManager.config_files.set_config_file as cfg_setter
app = typer.Typer()
set_app = typer.Typer()
app.add_typer(set_app, name="set")

@set_app.command()
def api_keys(section: str, public_key_var_name: str, public_key: str, private_key_var_name: str, private_key: str):
    success, msg = cfg_setter.set_keys(section,
    public_key_var_name,
    public_key,
    private_key_var_name,
    private_key)
    print_msg_typer(success, msg)

@app.command()
def reset():
    print_msg_typer(*(cfg_setter.reset_config()))

@app.command()
def show_config():
    content = cfg_setter.get_config_file_str()
    if not content:
        msg = 'EMPTY'
    else:
        msg = content
    typer.echo(msg)

def print_msg_typer(success, msg):
    if not success:
        typer.echo(f'ERROR: {msg}')
    else:
        typer.echo(f'SUCCESS: {msg}')

def main():
    app()