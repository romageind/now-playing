#define MyAppName "Now Playing"
#define MyAppVersion "1.0.0"
#define MyAppPublisher "Now Playing"
#define MyAppExeName "NowPlaying.exe"

[Setup]
AppId={{C4D8F7D1-5B8D-4F1C-91D8-72B4D3A11F20}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
DefaultDirName={localappdata}\Programs\NowPlaying
DefaultGroupName={#MyAppName}
UninstallDisplayIcon={app}\{#MyAppExeName}
ArchitecturesAllowed=x64
ArchitecturesInstallIn64BitMode=x64
PrivilegesRequired=lowest
OutputDir=..\dist\installer
OutputBaseFilename=NowPlaying-Setup-x64
SetupIconFile=..\assets\icon.ico
Compression=lzma2
SolidCompression=yes
WizardStyle=modern

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "Create a desktop shortcut"; GroupDescription: "Additional shortcuts:"; Flags: unchecked

[Files]
Source: "..\dist\NowPlaying\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{group}\Now Playing"; Filename: "{app}\{#MyAppExeName}"
Name: "{autodesktop}\Now Playing"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "Launch Now Playing"; Flags: postinstall nowait skipifsilent
