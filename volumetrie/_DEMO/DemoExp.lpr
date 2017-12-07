program DemoExp;

{$mode objfpc}{$H+}

uses
  {$IFDEF UNIX}{$IFDEF UseCThreads}
  cthreads,
  {$ENDIF}{$ENDIF}
  Interfaces, // this includes the LCL widgetset
  Forms, sdflaz, u_main, u_script, memdslaz, runtimetypeinfocontrols, u_ml,
  u_config, u_info;

{$R *.res}

begin
  RequireDerivedFormResource:=True;
  Application.Initialize;
  Application.CreateForm(Tfrm_Main, frm_Main);
  Application.CreateForm(Tfrm_Script, frm_Script);
  Application.CreateForm(Tfrm_Config, frm_Config);
  Application.CreateForm(Tfrm_info, frm_info);
  Application.Run;
end.

