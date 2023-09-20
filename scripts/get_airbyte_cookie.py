"""
This script prints the airbyte session cookies to the terminal that can be used to make calls
to airbyte server from local.

Usage:
Before running the file, login to the airbyte server from the chrome browser manually and then call this script.
This script will print the cookie name and value (if any) for various profiles you may have in the browser.
You can use any set of cookies to make calls to the airbyte server.

If you want to get the session cookies info for a particular profile, add a --cookie-file argument which will
take the absolute path of the cookie file where chrome has stored the cookies on local.

python scripts/get_token.py --cookie-file <absolute path to cookie file>(optional) --domain <domain name>(required)
    --pretty-print <Whether to print cookies found in a pretty format>
"""
import platform, argparse, os
from typing import List
import browser_cookie3

MAC_OS_COOKIE_FILE_PATH = os.environ.get('HOME') + '/Library/Application Support/Google/Chrome'
LINUX_COOKIE_FILE_PATH = os.environ.get('HOME') + '/.config/google-chrome'

# get the name of OS
def get_os_name() -> str:
    """
    Returns system's os name
    :return: System's OS name
    """
    return platform.system()


# get cookie file locations
def get_cookie_files(cookie_file: str = None) -> List[str]:
    """
    Returns file paths where cookies are stored by the browser.
    :param cookie_file: Cookie file path passed as an argument
    :return: List of file paths where to get cookie info from
    """
    os_name = get_os_name()
    cookie_files = []
    if cookie_file is None:
        if os_name == 'Linux':
            # default path
            default_file_path = LINUX_COOKIE_FILE_PATH + '/Default/Cookies'
            if os.path.exists(default_file_path):
                cookie_files.append(default_file_path)

            # if profiles exist
            cookie_files = cookie_files + [
                os.path.join(LINUX_COOKIE_FILE_PATH, f, 'Cookies') for f in os.listdir(LINUX_COOKIE_FILE_PATH)
                if (os.path.isdir(os.path.join(LINUX_COOKIE_FILE_PATH, f)) and 'Profile ' in f
                and os.path.exists(os.path.join(LINUX_COOKIE_FILE_PATH, f, 'Cookies')))
            ]
        elif os_name == 'Darwin':
            # default path
            default_file_path = MAC_OS_COOKIE_FILE_PATH + '/Default/Cookies'
            if os.path.exists(default_file_path):
                cookie_files.append(default_file_path)

            # if profiles exist
            cookie_files = cookie_files + [
                os.path.join(MAC_OS_COOKIE_FILE_PATH, f, 'Cookies') for f in os.listdir(MAC_OS_COOKIE_FILE_PATH)
                if (os.path.isdir(os.path.join(MAC_OS_COOKIE_FILE_PATH, f)) and 'Profile ' in f
                and os.path.exists(os.path.join(MAC_OS_COOKIE_FILE_PATH, f, 'Cookies')))
            ]
    else:
        cookie_files.append(cookie_file)

    return cookie_files


# print cookie name and value for airbyte domain
def get_cookie_info(cookie_files: List[str], domain_name: str, pretty_print: bool = False) -> None:
    """
    Prints cookie info
    :param cookie_files: File paths where cookies are stored by the browser
    :param domain_name: Domain name eg. 'airbyte-sandbox.re-sandbox.homestoryrewards.com'
    :param pretty_print: Boolean to indicate whether to print cookies found in a pretty format
    """
    print(f'============= DOMAIN NAME: {domain_name} =============')
    print('')
    for cookie_file_path in cookie_files:
        print(f'-------------- For cookie file: {cookie_file_path} ----------------')
        print('')
        cj = browser_cookie3.chrome(
            cookie_file=cookie_file_path,
            domain_name=domain_name
        )
        cookie_string = ''
        for cookie in cj:
            if 'AWSELBAuthSessionCookie' in cookie.name:
                if pretty_print:
                    print(f'{cookie.name}: {cookie.value}')
                    print('')
                else:
                    cookie_string += f'{cookie.name}={cookie.value};'

        if not pretty_print and cookie_string:
            print(cookie_string)
            print('')

def _define_arguments() -> argparse.Namespace:
    """
    Define all the named arguments which are required for script to run
    :return: (object) arguments object
    """
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--cookie-file', type=str, help='Cookie file location', required=False)
    parser.add_argument(
        '--domain',
        type=str,
        help='Domain name eg. airbyte-sandbox.re-sandbox.homestoryrewards.com',
        required=True
    )
    parser.add_argument(
        '--pretty-print',
        help='Prints cookies found in a pretty format',
        action='store_true',
        required=False
    )
    return parser.parse_args()


def main():
    """
    Main function to run local server for getting access and refresh token
    """
    args = _define_arguments()
    cookie_file = args.cookie_file
    domain_name=args.domain
    pretty_print = args.pretty_print
    cookie_files = get_cookie_files(cookie_file=cookie_file)
    get_cookie_info(cookie_files=cookie_files, domain_name=domain_name, pretty_print=pretty_print)


if __name__ == '__main__':
    main()
