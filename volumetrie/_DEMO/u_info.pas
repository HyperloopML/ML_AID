unit u_info;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, FileUtil, Forms, Controls, Graphics, Dialogs, Buttons,
  StdCtrls, ExtCtrls;

type

  { Tfrm_info }

  Tfrm_info = class(TForm)
    BitBtn1: TBitBtn;
    minf: TMemo;
    Panel1: TPanel;
    Panel2: TPanel;
  private
    { private declarations }
  public
    { public declarations }
  end;

var
  frm_info: Tfrm_info;

implementation

{$R *.lfm}

end.

