unit u_config;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, FileUtil, Forms, Controls, Graphics, Dialogs, StdCtrls,
  dynmatrixutils, dynmatrix, u_ml;

type

  { Tfrm_Config }

  Tfrm_Config = class(TForm)
    Memo1: TMemo;
    procedure FormCreate(Sender: TObject);
    procedure FormShow(Sender: TObject);
  private
    { private declarations }
  public
    { public declarations }
  end;

var
  frm_Config: Tfrm_Config;

implementation

{$R *.lfm}

{ Tfrm_Config }

procedure Tfrm_Config.FormCreate(Sender: TObject);
begin
end;

procedure Tfrm_Config.FormShow(Sender: TObject);
var
  X : TDMatrix;
  y : TDMatrix;
  y_hat: TDMatrix;
  regr : TLinearRegression;
  A,B, C : TDMatrix;
  start_t, end_t, i : longint;
  str_line : string;
begin
  X := StringToDMatrix('0 1 2; 0 2 1; 1 5 7; 0 3 5');
  y := StringToDMatrix('12 ; 21; 157; 35');
  memo1.Lines.Add('X:');
  MAddToStringList(X,memo1.Lines);
  memo1.Lines.add('y:');
  MAddToStringList(y,memo1.Lines);
  regr := TLinearRegression.Create;
  regr.fit(X,y);
  memo1.lines.add('Theta:');
  MAddToStringList(regr.GetTheta,memo1.Lines);
  memo1.lines.add('yhat:');
  y_hat := regr.predict(X);
  MAddToStringList(y_hat,memo1.Lines);

  A := Mrandom(1000,1000);
  str_line :='A[0,1:10] = ';
  for i:=1 to 10 do
   str_line := str_line + '  ' + FloatToStr(A.getv(0,i));
  memo1.lines.add(str_line);
  str_line :='B[0,1:10] = ';
  B := Mrandom(1000,1000);
  for i:=1 to 10 do
   str_line := str_line + '  ' + FloatToStr(B.getv(0,i));
  memo1.lines.add(str_line);
  start_t := TimeMs;
  C := A * B;
  end_t := TimeMs;
  memo1.lines.add('time A dot B = '+IntToStr(end_t-start_t));
  str_line :='C[0,1:10] = ';
  for i:=1 to 10 do
   str_line := str_line + '  ' + FloatToStr(C.getv(0,i));
  memo1.lines.add(str_line);


end;

end.

