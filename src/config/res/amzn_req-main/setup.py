from setuptools import setup, find_packages

description: str = """
AmznReq is a class designed specifically for facilitating communication with internal web applications at Amazon.
It combines Kerberos authentication with session-based cookie management to enable secure HTTP requests within Amazon's internal network.
Utilizing this class, developers can automate authentication processes for accessing Amazon's internal web pages or APIs that require authentication,
leveraging cookies exported from browsers or obtained from Selenium WebDriver.

Features include:
- Secure request execution through Kerberos authentication tailored for Amazon's internal systems.
- Capability to import cookies directly from the mwinit process.
- Importing cookies from major browsers (Chrome, Firefox, Edge) for seamless integration with personal browsing sessions.
- Ability to import cookies from Selenium WebDriver, facilitating automated testing and web scraping within Amazon's internal applications.
- Exporting session cookies, with support for formats compatible with Selenium WebDriver, enhancing testing automation.
- Functions to check and initialize Midway authentication status, ensuring access is securely managed.

Designed with Amazon's internal development ecosystem in mind, this class is particularly valuable for developers working on or with Amazon's internal web applications, providing essential tools for secure, efficient communication and testing within Amazon's corporate environment.
""".strip()

with open("requirements.txt") as f:
    install_requires = f.read().splitlines()

setup(
    name="AmznReq",
    version='1.0.1',
    author="Kohei Miyashita",
    author_email="mikohei@amazon.co.jp",
    description=description,
    packages=find_packages(),
    install_requires=install_requires,
)
