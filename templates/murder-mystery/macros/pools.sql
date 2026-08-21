{#
  Value pools for the Yorkville datasets.

  Yorkville is invented, and so is every name in it: no real country, city, region
  or person is referenced anywhere in the town. Pool sizes are chosen so that
  names repeat about as often as they do in a real town of this size — several
  residents share a surname, which is a fact the case relies on.
#}

{% set male_first_names = [
  'Alaric', 'Alder', 'Ansel', 'Arlen', 'Barnaby', 'Bertram', 'Caelan', 'Callum',
  'Caspar', 'Cedric', 'Corwin', 'Dorian', 'Dorrell', 'Drystan', 'Eamon', 'Edric',
  'Emmett', 'Everard', 'Ferris', 'Finnian', 'Garrick', 'Gideon', 'Halvard', 'Harlan',
  'Hollis', 'Ivor', 'Jarrett', 'Jorren', 'Kelvin', 'Kemper', 'Lachlan', 'Leland',
  'Lorcan', 'Malden', 'Merrick', 'Milo', 'Nathaniel', 'Norwood', 'Orrin', 'Osric',
  'Padraic', 'Perrin', 'Quillon', 'Quinton', 'Ramsey', 'Redmond', 'Roderick', 'Rowan',
  'Sefton', 'Silas', 'Sorrel', 'Tarquin', 'Thaddeus', 'Torin', 'Ulric', 'Vance',
  'Vidor', 'Warrick', 'Weston', 'Wilder', 'Yorick', 'Zachary', 'Zephyr', 'Balen'
] %}

{% set female_first_names = [
  'Adela', 'Ainsley', 'Alcina', 'Amity', 'Annora', 'Arvela', 'Beatrix', 'Bellamy',
  'Briony', 'Calla', 'Carys', 'Cerise', 'Clemency', 'Cordelia', 'Delphine', 'Dorcas',
  'Edwina', 'Elspeth', 'Emeline', 'Eulalia', 'Faye', 'Fenella', 'Flora', 'Frayda',
  'Gwenna', 'Halla', 'Harriet', 'Hester', 'Ilsa', 'Imogen', 'Isolde', 'Jessamine',
  'Junia', 'Katriona', 'Kestra', 'Lavinia', 'Leonie', 'Linnet', 'Maeve', 'Marisol',
  'Melisande', 'Merryn', 'Nerissa', 'Nolwenn', 'Odette', 'Ophira', 'Perpetua', 'Philippa',
  'Prudence', 'Quilla', 'Rhoswen', 'Romilly', 'Rosalind', 'Saffron', 'Selina', 'Sibyl',
  'Tamsin', 'Thea', 'Tressa', 'Ursula', 'Verity', 'Vivienne', 'Winifred', 'Zerlina'
] %}

{% set surnames = [
  'Ashby', 'Balfour', 'Barrow', 'Bellwether', 'Blackwood', 'Brack', 'Bramble', 'Bramwell',
  'Brightwell', 'Cadwell', 'Calloway', 'Carrick', 'Cassidy', 'Chalmers', 'Corbin', 'Cordray',
  'Cranmer', 'Crowther', 'Culpepper', 'Danforth', 'Darrow', 'Deering', 'Delacourt', 'Dennick',
  'Doverly', 'Dray', 'Dunmore', 'Eastleigh', 'Ebbing', 'Ellery', 'Emberly', 'Everstone',
  'Fallow', 'Farrant', 'Fenwick', 'Ferrin', 'Finchley', 'Flintlock', 'Foxbury', 'Frankley',
  'Gable', 'Garrow', 'Gattis', 'Glaive', 'Godwin', 'Grayling', 'Grimsby', 'Halloway',
  'Hambley', 'Hardacre', 'Harrow', 'Hasket', 'Hathorne', 'Havelock', 'Heddon', 'Hollis',
  'Hulme', 'Ingleby', 'Ironside', 'Jarret', 'Jessop', 'Kaldane', 'Keddle', 'Kessel',
  'Kilburn', 'Kinnaird', 'Lachman', 'Lambourne', 'Larkin', 'Ledbetter', 'Linden', 'Lorn',
  'Lowther', 'Mabry', 'Maddock', 'Mallory', 'Marchmont', 'Marrow', 'Mearns', 'Melrose',
  'Merriwether', 'Millbank', 'Morrow', 'Nadder', 'Nashwell', 'Netherby', 'Norrell', 'Nunnery',
  'Oakbourne', 'Odell', 'Orrick', 'Ostler', 'Padgett', 'Paltry', 'Parlow', 'Pell',
  'Pembrey', 'Penhale', 'Pettifer', 'Prendergast', 'Quarry', 'Quilter', 'Quint', 'Rackham',
  'Radleigh', 'Ravenhill', 'Redgrave', 'Renshaw', 'Ridley', 'Rookwood', 'Rowntree', 'Rushmere',
  'Sallow', 'Sarn', 'Scarsdale', 'Selby', 'Shackleford', 'Sheerwater', 'Shrike', 'Skelton',
  'Slaughter', 'Sowerby', 'Stallard', 'Stannard', 'Strang', 'Swithin', 'Tallowbrook', 'Tamworth',
  'Thackery', 'Thistlewood', 'Tolliver', 'Trencher', 'Trowbridge', 'Tyburn', 'Ulmer', 'Underhill',
  'Vance', 'Varden', 'Vellacott', 'Verity', 'Vickery', 'Volney', 'Wadlow', 'Wakeford',
  'Wardrup', 'Warrender', 'Weatherall', 'Wellbourne', 'Westray', 'Whitlock', 'Wickstead', 'Winterbourne',
  'Withersby', 'Wolstan', 'Woodrow', 'Wrayburn', 'Yarrow', 'Yelverton', 'Zell', 'Zorander',
  'Abernathy', 'Ackroyd', 'Aldington', 'Amberley', 'Anstruther', 'Applewhite', 'Arkwright', 'Attwater'
] %}

{% set districts = [
  'Nordheimer Vale', 'Wells Hill', 'Hillcrest', 'Bracondale',
  'Loma Heights', 'Wychwood', 'Davenport North', 'Seaton Green'
] %}

{% set streets = [
  'Alcina Avenue', 'Ardwold Gate', 'Barton Rise', 'Bathgate Lane', 'Benson Terrace', 'Boulton Walk', 'Bridgman Row', 'Burnside Drive',
  'Caledonia Park Road', 'Cardigan Walk', 'Cavendish Mews', 'Cecil Rise', 'Chaplin Crescent', 'Christie Row', 'Russell Hill Road', 'Clarendon Avenue',
  'Clifton Road', 'Colborne Lane', 'Cottingham Row', 'Croydon Walk', 'Dalton Mews', 'Deer Park Crescent', 'Delaware Row', 'Dewson Lane',
  'Dunvegan Road', 'Elmsthorpe Avenue', 'Elgin Mews', 'Everden Road', 'Farnham Avenue', 'Follis Lane', 'Fraser Rise', 'Garfield Walk',
  'Geneva Mews', 'Glenholme Row', 'Macpherson Mews', 'Gormley Lane', 'Hawthorn Gardens', 'Heath Street', 'Helena Avenue', 'Highbourne Road',
  'Hilton Avenue', 'Humewood Drive', 'Kendal Avenue', 'Spadina Crescent', 'Austin Terrace', 'Walmer Road', 'Lauder Walk', 'Lonsdale Road',
  'Lynwood Avenue', 'Madison Row', 'Marchmount Road', 'Maxwell Mews', 'Melgund Row', 'Davenport Road', 'Nina Street', 'Oakwood Rise',
  'Old Forest Lane', 'Oriole Parkway', 'Palmerston Gardens', 'Pinewood Walk', 'Poplar Plains Road', 'Rathnelly Avenue', 'Nordheimer Rise', 'Regal Road',
  'Ridge Hill Drive', 'Rosemount Row', 'Rushton Road', 'St Annes Walk', 'Shallmar Lane', 'Sherwood Rise', 'Springmount Avenue', 'Strathearn Row',
  'Tarragona Walk', 'Tichester Lane', 'Turner Road', 'Tyrrel Avenue', 'Ulster Row', 'Vaughan Road', 'Warren Crescent', 'Wells Street',
  'Westmount Walk', 'Whitaker Lane', 'Wickson Terrace', 'Willcocks Row', 'Winona Drive', 'Wolseley Mews', 'Wychwood Avenue', 'Yarmouth Gardens',
  'Yorkview Lane', 'Zina Walk'
] %}

{% set business_words_a = [
  'Alcina', 'Ardwold', 'Austin', 'Bathgate', 'Boulton',
  'Bracondale', 'Bridgman', 'Burnside', 'Caledonia', 'Cardigan',
  'Cavendish', 'Chaplin', 'Christie', 'Clarendon', 'Colborne',
  'Cottingham', 'Davenport', 'Dunvegan', 'Elmsthorpe', 'Everden',
  'Farnham', 'Follis', 'Glenholme', 'Hawthorn', 'Heath',
  'Highbourne', 'Hillcrest', 'Humewood', 'Kendal', 'Loma',
  'Lonsdale', 'Lynwood', 'Macpherson', 'Marchmount', 'Melgund',
  'Nordheimer', 'Oriole', 'Palmerston', 'Poplar Plains', 'Rathnelly',
  'Russell Hill', 'Seaton', 'Spadina', 'Strathearn', 'Tarragona',
  'Walmer', 'Wells Hill', 'Wychwood'
] %}

{% set business_words_b = [
  'Holdings', 'Works', 'Contracts', 'Supply', 'Trading', 'Services', 'Partners', 'Group',
  'Yard', 'Fabrication', 'Haulage', 'Joinery', 'Plant Hire', 'Roofing', 'Surveying', 'Fitters',
  'Provisions', 'Bakery', 'Grocers', 'Outfitters', 'Ironworks', 'Motors', 'Garage', 'Depot',
  'Clinic', 'Chambers', 'Studio', 'Press', 'Laundry', 'Removals'
] %}

{% set vehicle_models = [
  ['Corvid', 'Wren'], ['Corvid', 'Harrier'], ['Corvid', 'Kite'],
  ['Meridian', 'Sable'], ['Meridian', 'Larch'], ['Meridian', 'Foxglove'],
  ['Ostler', 'Tern'], ['Ostler', 'Plover'], ['Ostler', 'Curlew'],
  ['Anvil', 'Drover'], ['Anvil', 'Carter'], ['Anvil', 'Hauler'],
  ['Kestrel', 'Vireo'], ['Kestrel', 'Merlin'], ['Kestrel', 'Kestrel']
] %}


{# Trade types, each with the category a card terminal reports under. #}
{% set trade_types = [
  ['Grocers', 'grocery'], ['Provisions', 'grocery'], ['Bakery', 'grocery'],
  ['Butchers', 'grocery'], ['Fishmonger', 'grocery'], ['Deli', 'grocery'],
  ['Fuel', 'fuel'], ['Filling Station', 'fuel'], ['Services', 'fuel'],
  ['Arms', 'hospitality'], ['Tearooms', 'hospitality'], ['Bistro', 'hospitality'],
  ['Cafe', 'hospitality'], ['Tavern', 'hospitality'], ['Kitchen', 'hospitality'],
  ['Outfitters', 'outdoor goods'], ['Field Sports', 'outdoor goods'], ['Angling', 'outdoor goods'],
  ['Chemists', 'pharmacy'], ['Pharmacy', 'pharmacy'],
  ['Hardware', 'hardware'], ['Ironworks', 'hardware'], ['Timber', 'hardware'],
  ['Drapers', 'clothing'], ['Outfitting', 'clothing'], ['Shoes', 'clothing'],
  ['Cycles', 'sport'], ['Sports', 'sport'], ['Leisure', 'sport'],
  ['Motors', 'motor'], ['Tyres', 'motor'], ['Garage', 'motor'],
  ['Books', 'general'], ['Stationers', 'general'], ['Newsagent', 'general'],
  ['Florist', 'general'], ['Launderette', 'services'], ['Hairdressers', 'services'],
  ['Nurseries', 'general'], ['Pottery', 'general']
] %}
