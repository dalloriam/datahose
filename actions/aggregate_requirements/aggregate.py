from typing import List, Set

import os


def load_requirements(requirements_file: str) -> Set[str]:
    """
    Loads the set of dependencies from a single requirements file.
    Args:
        requirements_file (str): The absolute path to the requirements file.

    Returns:
        The set of dependencies defined in the file.
    """
    with open(requirements_file) as infile:
        return {req.strip() for req in infile.readlines()}


def aggregate_dependencies(root_dir: str) -> List[str]:
    """
    Walks root_dir recursively and collects all dependencies found in requirements.txt files.
    Args:
        root_dir (str): The root directory.

    Returns:
        The sorted list of all dependencies defined in the directory.
    """
    dependencies = set()
    for root, dirs, files in os.walk(root_dir):
        for f in files:
            if f == 'requirements.txt':
                req_path = os.path.join(root, f)
                print(f'Found requirements file -> [ {req_path} ]')
                dependencies.update(load_requirements(req_path))
    return sorted(list(dependencies))


def main():
    deps = aggregate_dependencies('/github/workspace')

    if os.path.isfile('/github/workspace/requirements.txt'):
        print('Root requirements.txt is already present... Renaming...')
        os.rename('/github/workspace/requirements.txt', '/github/workspace/requirements.old')

    with open('/github/workspace/requirements.txt', 'a') as req_file:
        req_file.write('\n'.join(deps))

    print(f'{len(deps)} dependencies found.')


if __name__ == '__main__':
    main()
