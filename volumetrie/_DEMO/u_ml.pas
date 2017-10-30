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
    procedure fit(X_train,y_train:TDMatrix);
    function predict(X_test:TDMatrix):TDMatrix;
    function GetTheta:TDMatrix;

end;

function TimeMs:longint;

implementation

function TimeMs: longint;
var ts: TTimeStamp;
begin
 ts := DateTimeToTimeStamp(Now);
 result := ts.Time;
end;

{ LinearRegression }

procedure TLinearRegression.fit(X_train, y_train: TDMatrix);
begin
  //Theta := MZeros(X_train.NumCols,1);
  Theta := (Minv(X_train.t * X_train) * X_train.t) * y_train;
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

