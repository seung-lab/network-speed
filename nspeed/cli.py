import click

from .nspeed import setup_test_files, run_speed_tests


class IntTuple(click.ParamType):
  """A command line option type consisting of 3 comma-separated integers."""
  name = 'tuple'
  def convert(self, value, param, ctx):
    if isinstance(value, str):
      try:
        value = tuple(map(int, value.split(',')))
      except ValueError:
        self.fail(f"'{value}' does not contain a comma delimited list of integers.")
    return value

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
@click.option('-p', '--parallel', default='1,8', type=IntTuple(), help="Comma delimited number of parallel processes to use.", show_default=True)
def transfer(source, dest, parallel):
    """(2) Run the speed tests."""
    run_speed_tests(
      sources = [source],
      dests = [ dest ],
      ncpu = parallel,
    )
