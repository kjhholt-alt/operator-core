' hidden-run.vbs - universal silent launcher for Windows Task Scheduler.
'
' Runs the command passed in its arguments with NO visible console window.
' Invoke via: wscript.exe hidden-run.vbs "C:\path\to\anything.bat" [args...]
'
' Why this exists: Task Scheduler tasks that run while the user is logged in
' flash a cmd window for .bat files even with "/it" flags. Routing through
' wscript.exe + WshShell.Run with window-style 0 suppresses that flash.

If WScript.Arguments.Count < 1 Then
    WScript.Quit 1
End If

Dim cmd, i
cmd = Chr(34) & WScript.Arguments(0) & Chr(34)
For i = 1 To WScript.Arguments.Count - 1
    cmd = cmd & " " & Chr(34) & WScript.Arguments(i) & Chr(34)
Next

Dim sh
Set sh = CreateObject("WScript.Shell")
' Run args: command, windowStyle (0 = hidden), waitForExit (True so Task
' Scheduler sees the real exit code).
WScript.Quit sh.Run(cmd, 0, True)
