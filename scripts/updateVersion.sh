#!/bin/bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

echo "The version is of form - Major.Minor.Micro-Modifier"
echo "Let's take v3.3.0-beta as an example. Here:"
echo "* Major is - 3"
echo "* Minor is - 3"
echo "* Micro is - 0"
echo "* Modifier is  - beta"
echo ""
echo "Now, enter the new version step-by-step below:"

version=""

# Major
read -p "* Major: " Major
if [ -z "$Major" ]
then
    echo "Major cannot be NULL"
    exit -1
fi
version+=$Major

# Minor
read -p "* Minor: " Minor
if [ -z "$Minor" ]
then
    echo "Minor cannot be NULL"
    exit -1
fi
version+="."$Minor

# Micro
read -p "* Micro: " Micro
if [ -z "$Micro" ]
then
    echo "Micro cannot be NULL"
    exit -1
fi
version+="."$Micro

# Modifier (optional)
read -p "* Modifier (optional, press enter if not needed): " Modifier
if [[ ! -z "$Modifier" ]]
then
    version+="-"$Modifier
fi

echo ""
echo "New version is: $version"

# update version in  ../params/version.go
versionFile="${DIR}/../db/version/app.go"
sed -i '' "s% = .*// Major% = $Major // Major%g" $versionFile
sed -i '' "s% = .*// Minor% = $Minor // Minor%g" $versionFile
sed -i '' "s% = .*// Micro% = $Micro // Micro%g" $versionFile
sed -i '' "s% = .*// Modifier% = \"$Modifier\" // Modifier%g" $versionFile
gofmt -w $versionFile

echo ""
echo "Updating Version Done"

exit 0
