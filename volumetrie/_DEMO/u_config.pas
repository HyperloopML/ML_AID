unit u_config;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, FileUtil, RTTICtrls, Forms, Controls, Graphics, Dialogs,
  StdCtrls, ComCtrls, ExtCtrls, Buttons, dynmatrixutils, dynmatrix,
  u_ml, u_script, math, u_info, DateUtils;

type

  { Tfrm_Config }

  Tfrm_Config = class(TForm)
    BitBtn1: TBitBtn;
    BitBtn2: TBitBtn;
    Label1: TLabel;
    Label2: TLabel;
    Label3: TLabel;
    Label4: TLabel;
    lbl_feats: TLabel;
    lbl_erori: TLabel;
    lbl_optici1: TLabel;
    lbl_statii: TLabel;
    lbl_optici: TLabel;
    Panel1: TPanel;
    Panel2: TPanel;
    Panel3: TPanel;
    Panel4: TPanel;
    TrackBar1: TTrackBar;
    TrackBar2: TTrackBar;
    TrackBar3: TTrackBar;
    TrackBar4: TTrackBar;
    procedure BitBtn1Click(Sender: TObject);
    procedure BitBtn2Click(Sender: TObject);
    procedure FormCreate(Sender: TObject);
    procedure FormShow(Sender: TObject);
    procedure TrackBar1Change(Sender: TObject);
    procedure TrackBar2Change(Sender: TObject);
    procedure TrackBar3Change(Sender: TObject);
    procedure TrackBar4Change(Sender: TObject);
  private
    { private declarations }

    function get_sensor: string;
  public
    { public declarations }
    nr_steps, prc_opt, nr_feats, nr_err : integer;

  end;

var
  frm_Config: Tfrm_Config;

implementation

{$R *.lfm}

{ Tfrm_Config }

procedure Tfrm_Config.FormCreate(Sender: TObject);
begin
end;

procedure Tfrm_Config.BitBtn1Click(Sender: TObject);
var
  i, i_step, i_start,j: integer;
  str_sensor_type,str_pred,str_true : string;
  X,y,yhat, coef : TDMatrix;
  reg : TMLPRegressorV2;
  f_pred,f_true, mse, rmse, erracc, prc, scale_factor,v:Double;
  i_err : integer;
  c_time : TDateTime;
begin

  nr_err:= StrToInt(lbl_erori.Caption);
  nr_steps := StrToInt(lbl_statii.Caption);
  nr_feats:=StrToInt(lbl_feats.Caption);
  prc_opt:=StrToInt(lbl_optici.Caption);

  frm_info.minf.Lines.Clear;
  frm_info.minf.Lines.Add('S-a pregatit o simulare cu '+IntToStr(nr_steps)+' pasi secventiali cu '
                               +IntToStr(nr_err)+' erori');

  //
  // create regression observations and feats
  // and add noise to labels (nr_err large noise values)
  //
  Randomize;
  X := MRandom(nr_steps,nr_feats);
  for i:=0 to X.NumRows-1 do
  begin
    for j:=1 to 10 do
    begin
      v := X.getv(i,j);
      v := v * (((i*2) mod 20)+random);
      X.setv(i,j,v);
    end;
  end;
  y := GenerateLabels(X, coef);

  // add small noise for half observations
  i_step := 2;
  i_start := 1;
  for i:=1 to nr_err do
  begin
    f_true := y.getv(i_start,0);
    if(i_start<y.NumRows) then
        y.setv(i_start,0,f_true+(Random(100)/100)+5);
    i_start := i_start + i_step;
  end;


  //
  // now train and predict dataset
  //
  //
  reg := TMLPRegressorV2.Create;
  // train on generated data and timings
  reg.fit(X,y);
  // generate predictions based on trained model
  yhat := reg.predict(X);

  //
  // add errors to ground-truth (simulate problems)
  //
  i_step := (nr_steps div nr_err);
  i_start := 10;
  for i:=1 to nr_err do
  begin
    f_true := y.getv(i_start,0);
    if (i_start<y.NumRows) then
        y.setv(i_start,0,f_true+(Random(100)/100)+Random(100)+Random(50)+Random(10));
    i_start := i_start + i_step;
  end;

  //
  // now create de dataset with real labels and predicted labels
  // frm_script.ds_csv
  //
  frm_script.ds_csv.Active := False;
  frm_script.ds_csv.FileName:='data.csv';
  frm_script.ds_csv.Active := True;
  frm_script.ds_source.Active := False;
  frm_script.ds_source.CopyFromDataset(frm_script.ds_csv,False);
  frm_script.ds_source.Active := True;
  with frm_script.ds_source do
  begin
    c_time := Now;
    for i:=1 to nr_steps do
    begin
      str_sensor_type := get_sensor;
      f_pred := RoundTo(yhat.getv(i-1,0),-2);
      f_true := RoundTo(y.getv(i-1,0),-2);
      str_pred := FloatToStr(f_pred);
      str_true := FloatToStr(f_true);

      if abs(f_pred-f_true)> f_true*0.125 then
        begin
          i_err := 0;
        end
        else
        begin
          i_err := 1;
        end;

      mse := sqr(f_pred-f_true);
      rmse := sqrt(mse);
      erracc:= f_pred*0.12;
      prc := rmse / f_pred;


      Append;

      FieldByName('Field2').AsString := IntToStr(Random(100000)+100000);
      FieldByName('Field1').AsString := IntToStr(i);
      FieldByName('Field4').AsString := str_sensor_type;
      FieldByName('Field12').AsInteger := i_err;
      FieldByName('Field3').AsString := IntToStr(Random(100000)+300000);;
      FieldByName('Field6').AsString := str_pred;  ///// PREDICTED
      FieldByName('Field7').AsString := str_true;  ///// GROUND TRUTH

      FieldByName('Field5').AsString := FormatDateTime('YYYY-MM-DD hh:nn:ss',c_time);

      FieldByName('Field8').AsString := Format('%3.2f',[mse]);
      FieldByName('Field9').AsString := Format('%3.2f',[rmse]);
      FieldByName('Field10').AsString := Format('%3.2f',[erracc]);
      FieldByName('Field11').AsString := Format('%3.2f',[prc]);


      c_time := IncMilliSecond(c_time,round(f_true));

      Post;
    end;
  end;

  frm_script.ds_source.First;

  frm_Script.Color:=clAqua;

  frm_info.minf.Lines.Add('Matricea predictorilor de dim Observatii X Nr.predictori ('+
                          IntToStr(nr_steps)+' X '+IntToStr(nr_feats)+'):');
  MAddToSL(X,frm_info.minf.Lines,'%6.2f',',');

  frm_info.minf.Lines.Add('Matricea rezultatelor reale ale masurarilor de timpi:');
  MAddToSL(y,frm_info.minf.Lines,'%6.2f',',');

  frm_info.minf.Lines.Add('Matricea predictiilor modelului de Machine Learning:');
  MAddToSL(yhat,frm_info.minf.Lines,'%6.2f',',');

  frm_info.minf.Lines.Add('Matricea modelului generativ:');
  MAddToSL(coef,frm_info.minf.Lines,'%6.2f',',');

  frm_info.minf.Lines.Add('Matricea matricea coeficientilor modelului de Machine Learning bazata pe masuratori si predictori:');
  MAddToSL(reg.GetTheta,frm_info.minf.Lines,'%6.2f',',');


  frm_info.ShowModal;
  frm_Script.ShowModal;
end;

procedure Tfrm_Config.BitBtn2Click(Sender: TObject);
begin

end;

procedure Tfrm_Config.FormShow(Sender: TObject);
begin
end;

procedure Tfrm_Config.TrackBar1Change(Sender: TObject);
begin
  lbl_statii.caption := IntToStr(TrackBar1.Position);
end;

procedure Tfrm_Config.TrackBar2Change(Sender: TObject);
begin
  lbl_optici.Caption:=IntToStr(TrackBar2.Position);
end;

procedure Tfrm_Config.TrackBar3Change(Sender: TObject);
begin
  lbl_feats.Caption := IntToStr(TrackBar3.Position);
end;

procedure Tfrm_Config.TrackBar4Change(Sender: TObject);
begin
  lbl_erori.Caption := IntToStr(TrackBar4.Position);
end;

function Tfrm_Config.get_sensor: string;
var i_nr : integer;
  res :string;
begin
  i_nr := Random(100);
  if i_nr <= prc_opt then
    res := 'O'
  else
    res := 'M';
  result := res;
end;

end.

