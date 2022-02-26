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

@app.command()
def uninstall():
    print_msg_typer(*(cfg_setter.delete_temp_files()))

@app.command()
def gdrive_client_secrets():
    typer.echo('Follow https://stackoverflow.com/questions/28184419/pydrive-invalid-client-secrets-file to get your Google Drive credentials\n')
    contents = str(input('Paste the contents of your client_sercrets.json here: '))
    print_msg_typer(*(cfg_setter.gdrive_client_secrets(contents)))

def print_msg_typer(success, msg):
    if not success:
        typer.echo(f'ERROR: {msg}')
    else:
        typer.echo(f'SUCCESS: {msg}')

@app.command()
def upload_files():
    pass

@app.command()
def download_files():
    pass

def main():
    app()