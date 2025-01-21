import click

from .nspeed import setup_test_files, run_speed_tests

@click.group("main")
def cli_main():
  """
  Test the speed of various transfer configurations.
  """
  pass

@cli_main.command("init")
@click.argument("sources", nargs=-1)
def init(sources):
  """(1) Populate source locations with test files."""

  if len(sources):
    setup_test_files(sources)
  else:
    setup_test_files()

@cli_main.command("send")
def send():
    """(2) Run the speed tests."""
    run_speed_tests()

@cli_main.command("transfer")
@click.argument("source")
@click.argument("dest")
def transfer(source, dest):
    """(2) Run the speed tests."""
    run_speed_tests(
      sources = [source],
      dests = [ dest ],
    )
