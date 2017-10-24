unit u_main;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, SdfData, db, FileUtil, Forms, Controls, Graphics, Dialogs,
  Menus, DBGrids, Buttons, ExtCtrls, StdCtrls, u_script;

type

  { Tfrm_Main }

  Tfrm_Main = class(TForm)
    BitBtn1: TBitBtn;
    BitBtn2: TBitBtn;
    Memo1: TMemo;
    Panel1: TPanel;
    Panel2: TPanel;
    procedure Bevel1ChangeBounds(Sender: TObject);
    procedure BitBtn1Click(Sender: TObject);
    procedure BitBtn2Click(Sender: TObject);
    procedure FormCreate(Sender: TObject);
    procedure Memo1Change(Sender: TObject);
    procedure Panel1Click(Sender: TObject);
  private
    { private declarations }
  public
    { public declarations }
  end;

var
  frm_Main: Tfrm_Main;



implementation

{$R *.lfm}

{ Tfrm_Main }

procedure Tfrm_Main.FormCreate(Sender: TObject);
begin
end;

procedure Tfrm_Main.Memo1Change(Sender: TObject);
begin

end;

procedure Tfrm_Main.Panel1Click(Sender: TObject);
begin

end;

procedure Tfrm_Main.BitBtn2Click(Sender: TObject);
begin

end;

procedure Tfrm_Main.Bevel1ChangeBounds(Sender: TObject);
begin

end;

procedure Tfrm_Main.BitBtn1Click(Sender: TObject);
begin
  frm_Script.ShowModal;
end;

end.

