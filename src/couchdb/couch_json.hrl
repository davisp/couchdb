
% Characters
-define(DQ, 34).    % Double quote
-define(PL, 43).    % Plus sign
-define(CM, 44).    % Comma
-define(HY, 45).    % Hyphen
-define(PR, 46).    % Period
-define(FS, 47).    % Forward solidus
-define(ZR, 48).    % Zero
-define(NI, 57).    % Nine
-define(CL, 58).    % Colon
-define(UE, 69).    % E
-define(LB, 91).    % Left bracket
-define(RS, 92).    % Reverse solidus
-define(RB, 93).    % Right bracket
-define(LE, 101).   % e
-define(LC, 123).   % Left curly brace
-define(RC, 125).   % Right curly brace

% Escapes
-define(ESDQ, 16#5C22). % \"
-define(ESRS, 16#5C5C). % \\
-define(ESFS, 16#5C2F). % \/
-define(ESBS, 16#5C62). % \b
-define(ESFF, 16#5C66). % \f
-define(ESNL, 16#5C6E). % \n
-define(ESCR, 16#5C72). % \r
-define(ESTB, 16#5C74). % \t
-define(ESUE, 16#5C75). % \u

% Whitespace
-define(BS, 8).     % Backspace
-define(TB, 9).     % Tab
-define(NL, 10).    % New line
-define(FF, 12).    % Form feed
-define(CR, 13).    % Carriage return
-define(SP, 32).    % Space

% Error
-define(EXIT(E), throw({error, E})).