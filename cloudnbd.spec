%define name    cloudnbd
%define version 0.1
%define release 1

Name:           %{name}
Version:        %{version}
Release:        %{release}
Summary:        NBD server with cloud storage as backend

Group:          Development/Libraries
License:        GPL
Source0:        %{name}-%{version}.tar.bz2
Vendor:         Mansour <mansour@oxplot.com>
URL:            https://github.com/oxplot/cloudnbd

BuildArch:      noarch
BuildRequires:  python
Requires:       python

%description
NBD server with cloud storage as backend.

%prep
%setup -n %{name}-%{version}

%build
%{__python} setup.py build

%install
%{__python} setup.py install -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES

%clean
rm -rf $RPM_BUILD_ROOT

%files -f INSTALLED_FILES
%defattr(-,root,root)
%doc README LICENSE ChangeLog AUTHORS TODO

%changelog
* Fri Oct 29 2010 Mansour <mansour@oxplot.com> - 0.1-1
- Initial release
