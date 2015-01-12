# Copyright 1999-2014 Gentoo Foundation
# Distributed under the terms of the GNU General Public License v2
# $Header: $
# Ebuild generated by g-pypi 0.3

EAPI="3"
SUPPORT_PYTHON_ABIS="1"
DISTUTILS_SRC_TEST="setup.py"

inherit distutils git-r3

EGIT_REPO_URI="git://github.com/sjlongland/stormed-amqp.git"
EGIT_COMMIT="v${PV}"

DESCRIPTION="native tornadoweb amqp 0-9-1 client implementation"
HOMEPAGE="UNKNOWN"
SRC_URI=""

LICENSE=""
KEYWORDS="~amd64"
SLOT="0"
IUSE="examples"

DEPEND="dev-python/setuptools"
RDEPEND="${DEPEND}"


src_install() {
	distutils_src_install
	if use examples; then
		insinto /usr/share/doc/"${PF}"/
		doins -r examples
	fi
}
