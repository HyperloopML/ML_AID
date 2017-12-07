unit u_ml;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, dynmatrix, dynmatrixutils;

type

{ LinearRegression }

 { TLinearRegression }

 TLinearRegression=class(TObject)
    Theta : TDMatrix;
  public
    procedure fit(X_train,y_train:TDMatrix; lambda:double = 0.5);
    function predict(X_test:TDMatrix):TDMatrix;
    function GetTheta:TDMatrix;

end;

type TMLPRegressorV2 = TLinearRegression;

function TimeMs:longint;
function GenerateLabels(X: TDMatrix; var coefs: TDMatrix; scale_factor:double = 0.2 ): TDMatrix;

implementation

function TimeMs: longint;
var ts: TTimeStamp;
begin
 ts := DateTimeToTimeStamp(Now);
 result := ts.Time;
end;

function GenerateLabels(X: TDMatrix; var coefs: TDMatrix; scale_factor:double = 0.2): TDMatrix;
begin
 coefs := Mrandom(X.NumCols,1) * scale_factor;
 result := X * coefs;
end;

{ LinearRegression }

procedure TLinearRegression.fit(X_train, y_train: TDMatrix; lambda:double = 0.5);
var
  n : integer;
begin
  n := X_train.NumCols;
  Theta := (Minv(X_train.t * X_train + lambda*Meye(n)) * X_train.t) * y_train;
end;

function TLinearRegression.predict(X_test: TDMatrix): TDMatrix;
begin
 result := X_test * Theta;
end;

function TLinearRegression.GetTheta: TDMatrix;
begin
  result := Theta;
end;

end.

