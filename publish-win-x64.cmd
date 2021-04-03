del ..\publish-win-x64\* /F /Q
dotnet clean --configuration Release --runtime win-x64
dotnet publish --configuration Release --runtime win-x64 --self-contained --output ..\publish-win-x64