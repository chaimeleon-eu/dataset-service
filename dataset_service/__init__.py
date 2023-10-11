# Dataset Service
# Copyright (C) 2021 - GRyCAP - Universitat Politecnica de Valencia
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import os
version_file_path = os.path.realpath(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'VERSION'))
with open(version_file_path, 'r') as file: version = file.readline()

__all__ = ['config', 'RESTServer']
__appname__ = 'Dataset Service'
__version__ = version
__author__ = 'GRYCAP-UPV'
__email__ = 'palollo@i3m.upv.es'
