import os
import setuptools
import sys

setuptools.setup(
  setup_requires=['pbr'],
  python_requires=">=3.8,<4.0",
  include_package_data=True,
  entry_points={
    "console_scripts": [
      "nspeed=nspeed:cli_main"
    ],
  },
  pbr=True
)

