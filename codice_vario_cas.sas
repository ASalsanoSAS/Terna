/*Fattore di replica */
data p.mytable_upd (copies=2);
   set p.mytable;
run;

/*Ricaricare da sashdat*/
proc cas;
action table.loadTable / path="&coutrymaiusci..sashdat", caslib="&outcaslib.", casOut={name="&coutrymaiusci.", caslib="&outcaslib.", replace=false, replication=0, promote=true};
run;
/*LOAD E SAVE INSIEME */
proc casutil;
load data=&INLIB..&dsi. outcaslib="&CASLIB" casout="&dso" NCHARMULTIPLIER=3 promote;
save casdata="&dso." incaslib="&CASLIB" outcaslib="&CASLIB" casout="&dso" replace;
quit;

