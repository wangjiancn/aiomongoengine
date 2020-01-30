### First time setup

- Download and install the [latest version of
git](https://git-scm.com/downloads).

- Configure git with your [username](https://help.github.) and
[email]():

    git config --global user.name 'your name'
    git config --global user.email 'your email'

- Make sure you have a [GitHub account](https://github.com/join).

- Fork aioMongoengine to your GitHub account by clicking the [Fork]() button.

- [Clone]() your GitHub fork locally:

    git clone https://github.com/{username}/aiomongoengine
    cd flask

- Add the main repository as a remote to update later:

    git remote add pallets https://github.com/pallets/aiomongoengine
    git fetch pallets

- Create a virtualenv:

    python3 -m venv env
    . env/bin/activate
    # or "env\Scripts\activate" on Windows

- Install Flask in editable mode with development dependencies:

    pip install -e ".[dev]"

- Install the [pre-commit framework]().

- Install the pre-commit hooks:

    pre-commit install --install-hooks