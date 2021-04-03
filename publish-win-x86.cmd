del ..\publish-win-x86\* /F /Q
dotnet clean --configuration Release --runtime win-x86
dotnet publish --configuration Release --runtime win-x86 --self-contained --output ..\publish-win-x86