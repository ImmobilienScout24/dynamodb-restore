from pybuilder.core import use_plugin, init

use_plugin("python.core")
use_plugin("python.unittest")
use_plugin("python.install_dependencies")
use_plugin("python.flake8")
use_plugin("python.coverage")
use_plugin("python.distutils")
use_plugin('copy_resources')


name = "dynamodb-restore"
default_task = "publish"
version = 1.0


@init
def set_properties(project):
    project.depends_on("boto3")
    project.depends_on("docopt")
    project.build_depends_on("mock")
    project.set_property('coverage_break_build', False)
    project.set_property('install_dependencies_upgrade', True)

    project.set_property('copy_resources_target', '$dir_dist')
    project.get_property('copy_resources_glob').append('setup.cfg')

    project.set_property('distutils_console_scripts', ['dynamodb-restore=dynamodb_restore.cli:main'])

    project.set_property('distutils_classifiers', [
    'Development Status :: 4 - Beta',
    'Environment :: Console',
    'Intended Audience :: Developers',
    'Intended Audience :: System Administrators',
    'License :: OSI Approved :: Apache Software License',
    'Programming Language :: Python',
    'Topic :: System :: Systems Administration'
])

@init(environments='teamcity')
def set_properties_for_teamcity_builds(project):
    import os

    project.set_property('teamcity_output', True)
    project.version = '%s-%s' % (project.version, os.environ.get('BUILD_NUMBER', 0))
    project.default_task = ['clean', 'install_build_dependencies', 'publish']
    project.set_property('install_dependencies_index_url', os.environ.get('PYPIPROXY_URL'))
    project.get_property('distutils_commands').append('bdist_rpm')