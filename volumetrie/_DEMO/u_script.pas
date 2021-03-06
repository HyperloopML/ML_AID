unit u_script;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, SdfData, memds, db, FileUtil, Forms, Controls, Graphics,
  Dialogs, DBGrids, StdCtrls, ExtCtrls, Buttons, ColorBox, Grids, ComCtrls,
  Types, LCLType, Windows;

type GridCol=record
                   ColName:string;
                   ColSize:integer;
end;

type TLogEntry=record
  str_text : string;
  clr : TColor;
  idx : integer;
end;

type

  { Tfrm_Script }

  Tfrm_Script = class(TForm)
    btn_start: TBitBtn;
    BitBtn2: TBitBtn;
    BitBtn3: TBitBtn;
    BitBtn4: TBitBtn;
    dsrc_log: TDataSource;
    dg_grid: TDBGrid;
    ds_source: TMemDataset;
    GroupBox1: TGroupBox;
    ds_csv: TSdfDataSet;
    ds_log: TMemDataset;
    GroupBox2: TGroupBox;
    GroupBox3: TGroupBox;
    Label1: TLabel;
    Label2: TLabel;
    Label3: TLabel;
    Label4: TLabel;
    Label5: TLabel;
    Label6: TLabel;
    lb_log: TListBox;
    pnl_act: TPanel;
    pnl_opt: TPanel;
    pnl_mag: TPanel;
    pnl_dnn: TPanel;
    pnl_err: TPanel;
    tmr_script: TTimer;
    sld_timer: TTrackBar;
    procedure BitBtn1Click(Sender: TObject);
    procedure btn_startClick(Sender: TObject);
    procedure BitBtn2Click(Sender: TObject);
    procedure BitBtn3Click(Sender: TObject);
    procedure BitBtn4Click(Sender: TObject);
    procedure FormClose(Sender: TObject; var CloseAction: TCloseAction);
    procedure FormCreate(Sender: TObject);
    procedure FormShow(Sender: TObject);
    procedure GroupBox1Click(Sender: TObject);
    procedure GroupBox3Click(Sender: TObject);
    procedure Label3Click(Sender: TObject);
    procedure lb_logClick(Sender: TObject);
    procedure lb_logDragOver(Sender, Source: TObject; X, Y: Integer;
      State: TDragState; var Accept: Boolean);
    procedure lb_logDrawItem(Control: TWinControl; Index: Integer;
      ARect: TRect; State: TOwnerDrawState);
    procedure sld_timerChange(Sender: TObject);
    procedure tmr_scriptTimer(Sender: TObject);
  private
    { private declarations }
    in_timer : boolean;
    new_start:boolean;
    log_list : array[0..512] of TLogEntry;
    log_list_count : integer;
    demo_done : boolean;

    i_opt,i_mag,i_dnn,i_err,i_act: integer;

    procedure add_log(str_text:string; clr:TColor);
    procedure clear_log;
    procedure restart_demo;
    procedure update_stats;
  public
    { public declarations }

    procedure ClearDataset;

  end;



var
  frm_Script: Tfrm_Script;

const CLR_ERROR = TColor($b9c1f7);
const CLR_OK = TColor($ffe8cc);




const fields: array[0..11] of GridCol =
  ((ColName:'ID'; ColSize:25),//Field1
   (ColName:'SenzorID'; ColSize:60), //Field2
   (ColName:'IdPers'; ColSize:50), //Field3
   (ColName:'TipSenzor'; ColSize:50), //Field4
   (ColName:'Time'; ColSize:110), //Field5
   (ColName:'TimpPrezis'; ColSize:70),//Field6
   (ColName:'TimeDelta'; ColSize:70),//Field7
   (ColName:'SQR(ERR)'; ColSize:70),//Field8
   (ColName:'SQRT(ERR)'; ColSize:70),//Field9
   (ColName:'ErrAcceptabila'; ColSize:70),//Field10
   (ColName:'Pred%'; ColSize:50),//Field11
   (ColName:'Accept'; ColSize:50) //Field12
   );


implementation

{$R *.lfm}

{ Tfrm_Script }

procedure Tfrm_Script.FormCreate(Sender: TObject);
begin
end;

procedure Tfrm_Script.FormShow(Sender: TObject);
var i:integer;
begin
  i_opt := 0;
  i_mag := 0;
  i_dnn := 0;
  i_err := 0;
  i_act := 0;

  new_start := True;
  in_timer := False;

  ds_log.CopyFromDataset(ds_source, False);
  ds_log.Active := True;
  for i:=0 to dg_grid.Columns.Count-1 do
  begin
    dg_grid.Columns[i].Width:= fields[i].ColSize;
    dg_grid.Columns[i].Title.Caption := fields[i].ColName;
    dg_grid.Columns[i].Alignment:= TAlignment.taRightJustify;
  end;

end;

procedure Tfrm_Script.BitBtn2Click(Sender: TObject);
begin
 tmr_script.Enabled := False;
 restart_demo;
 tmr_script.Enabled := True;
end;

procedure Tfrm_Script.btn_startClick(Sender: TObject);
begin
 if demo_done then
 begin
   restart_demo;
 end;
 tmr_script.Enabled := True;
end;

procedure Tfrm_Script.BitBtn1Click(Sender: TObject);
begin
// lb_log.Perform(WM_VSCROLL,SB_BOTTOM,0);
// lb_log.Perform(WM_VSCROLL,SB_ENDSCROLL,0);
// lb_log.Perform(LB_SETCURSEL ,lb_log.Items.Count-1,MakeLParam(WORD(false), 0));
 lb_log.ItemIndex:= lb_log.Count - 1;
// lb_log.TopIndex:= lb_log.Count - 1;

end;

procedure Tfrm_Script.BitBtn3Click(Sender: TObject);
begin
  tmr_script.Enabled := False;
end;

procedure Tfrm_Script.BitBtn4Click(Sender: TObject);
begin
  Close;
end;

procedure Tfrm_Script.FormClose(Sender: TObject; var CloseAction: TCloseAction);
begin
 tmr_script.Enabled := False;
 ds_source.Active := False;;
 ds_log.Active:=False;;
 lb_log.Items.Clear;
end;

procedure Tfrm_Script.GroupBox1Click(Sender: TObject);
begin

end;

procedure Tfrm_Script.GroupBox3Click(Sender: TObject);
begin

end;

procedure Tfrm_Script.Label3Click(Sender: TObject);
begin

end;

procedure Tfrm_Script.lb_logClick(Sender: TObject);
begin

end;

procedure Tfrm_Script.lb_logDragOver(Sender, Source: TObject; X, Y: Integer;
  State: TDragState; var Accept: Boolean);
begin

end;

procedure Tfrm_Script.lb_logDrawItem(Control: TWinControl; Index: Integer;
  ARect: TRect; State: TOwnerDrawState);
var
  clr: TColor;
  str_text : string;
  log_entry : TLogEntry;
  tmpStyle : TTextStyle;
begin
  with (Control as TListBox).Canvas do
  begin
    log_entry  := log_list[Index];
    clr := log_entry.clr;
    str_text := log_entry.str_text;
    Brush.Style := bsSolid;
    Brush.Color := clr;
    FillRect(ARect);
    Brush.Style := bsClear;
    if (odFocused in State) or (odSelected in State) then
    begin
     Font.Color := clBlue;
    end;
    Pen.Style := psClear;
    tmpStyle := TextStyle;
    tmpStyle.Wordbreak := True;
    tmpStyle.SingleLine := False;
    TextRect(ARect,ARect.Left,ARect.Top,str_text,tmpStyle);
    if odFocused in State then
    begin
      DrawFocusRect(ARect);
    end;
  end;
end;

procedure Tfrm_Script.sld_timerChange(Sender: TObject);
begin
  tmr_script.Interval := sld_timer.Position;
end;

procedure Tfrm_Script.tmr_scriptTimer(Sender: TObject);
var
  i : integer;
  src_val : Variant;
  src_name : string;
  str_senzor: string;
  str_tip_senzor: string;
  str_operator : string;
  str_time_pred,str_time_real:string;
  str_log:string;
  is_error : integer;
  clr : TColor;
begin
 if in_timer or demo_done then
    exit;
 in_timer := True;
 if not ds_source.EOF then
 begin
   if new_start then
   begin
     log_list_count := 0;
     add_log('DEMO STARTED! Se foloseste tota virtuala #12345 pe un '+
             'traseu virtual de banda industriala cu statii multiple '+
             'supervizata de WSN+DNN',clGreen);
     new_start := False;
   end;
   ds_log.Append;
   for i:=0 to ds_source.Fields.Count-1 do
   begin
     src_val := ds_source.Fields[i].Value;
     src_name := ds_source.Fields[i].FieldName;
     ds_log.Fields[i].Value := src_val;
   end;
   //
   str_senzor := ds_source.FieldByName('Field2').AsString;
   str_tip_senzor := ds_source.FieldByName('Field4').AsString;
   is_error := ds_source.FieldByName('Field12').AsInteger;
   str_operator := ds_source.FieldByName('Field3').AsString;
   str_time_pred := ds_source.FieldByName('Field6').AsString;
   str_time_real := ds_source.FieldByName('Field7').AsString;
   //
   if UpperCase(str_tip_senzor) = 'M' then
   begin
      str_tip_senzor := 'magnetic';
      inc(i_mag)
   end
   else
   begin
      str_tip_senzor := 'optic';
      inc(i_opt)
   end;

   inc(i_dnn);

   if is_error = 0 then
   begin
     clr := CLR_ERROR;
     str_log := 'Senzorul '+str_tip_senzor+' ID' +str_senzor+' a inregistrat tota #12345 in zona statiei operatorului '
             +str_operator+'. Predictia retelei neurale adanci de '
             +str_time_pred+'s este in contradictie cu timpul real determinat de WSN de '
             +str_time_real+'s.';
     inc(i_err)
   end
   else
   begin
     clr := CLR_OK;
     str_log := 'Senzorul '+str_tip_senzor+' ID' +str_senzor+' a inregistrat tota #12345 in zona statiei operatorului '
             +str_operator+'. Reteaua neurala a facut o predictie a timpului de procesare de '
             +str_time_pred+'s comparabila cu timpul real determinat de reteaua de senzori de '
             +str_time_real+'s.';
     inc(i_act)
   end;

   add_log(str_log,clr);
   ds_log.Post;
   ds_source.Next;
 end
 else
 begin
   add_log('END DEMO ACTIONS.',clGreen);
   demo_done := True;
   tmr_script.Enabled := False;
 end;
 lb_log.ItemIndex:= lb_log.Count - 1;
 update_stats;
 in_timer := False;
end;

procedure Tfrm_Script.add_log(str_text: string; clr: TColor);
var
  log_entry : TLogEntry;
begin
  if clr = CLR_ERROR then
    str_text := 'Anomalie, se recomanda interventie! '#13#10 + str_text;

  log_entry.clr := clr;
  log_entry.idx := log_list_count;
  log_entry.str_text := str_text;

  log_list[log_list_count] := log_entry;
  inc(log_list_count);

  lb_log.Items.Add(str_text);
end;

procedure Tfrm_Script.clear_log;
begin
  log_list_count := 0;
  lb_log.Clear;
end;

procedure Tfrm_Script.restart_demo;
begin
 ds_source.First;
 ds_log.Clear(False);
 clear_log;
 new_start := True;
 demo_done := False;

  i_opt := 0;
  i_mag := 0;
  i_dnn := 0;
  i_err := 0;
  i_act := 0;

  update_stats;

end;

procedure Tfrm_Script.update_stats;
begin
  pnl_err.Caption:= IntTostr(i_err);
  pnl_act.Caption:= IntTostr(i_act);
  pnl_dnn.Caption:= IntTostr(i_dnn);
  pnl_opt.Caption:= IntTostr(i_opt);
  pnl_mag.Caption:= IntTostr(i_mag);
end;

procedure Tfrm_Script.ClearDataset;
begin
  ds_source.First;
  while NOT ds_source.EOF do
  begin
   ds_source.Delete;
  end;
end;

end.

