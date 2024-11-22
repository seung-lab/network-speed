import click

from .nspeed import setup_test_files, run_speed_tests

@click.group("main")
def cli_main():
  """
  Test the speed of various transfer configurations.
  """
  pass

@cli_main.command("init")
def init():
  """(1) Populate source locations with test files."""
  setup_test_files()

@cli_main.command("run")
def run():
    """(2) Run the speed tests."""
    run_speed_tests()