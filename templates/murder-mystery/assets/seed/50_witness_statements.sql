/* @bruin
name: casefile.witness_statements
description: |
  Statements taken by responding officers in the twelve hours after the shooting.
  Witnesses describe what they saw; none of them knew who they were looking at.
materialization:
  type: table
columns:
  - name: statement_id
    type: varchar
    description: Statement identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: taken_at
    type: timestamp
    description: When the statement was taken
  - name: witness_ref
    type: varchar
    description: Reference the officer logged the witness under
  - name: location
    type: varchar
    description: Where the witness was standing
  - name: statement
    type: varchar
    description: What the witness said, as written down
@bruin */

SELECT
    'WS-' || lpad(n::VARCHAR, 3, '0') AS statement_id,
    ({{ rally_date() }}::TIMESTAMP + INTERVAL 19 HOUR + INTERVAL (n * 17) MINUTE) AS taken_at,
    witness_ref,
    location,
    statement
FROM (VALUES
    ( 1, 'W-01', 'Wychwood Square, by the civic steps', 'The bang came from up and behind me, off to the north side. Everyone went down at once. I did not see a flash.'),
    ( 2, 'W-02', 'Wychwood Square, near the platform', 'He was mid-sentence and then he was not. I was looking straight at him. There was no second shot.'),
    ( 3, 'W-03', 'Austin Terrace, outside the bakery', 'I noticed a man in a dark work jacket walk up the side of the Loma House and go in at the door there. Just after five past six. He was not hurrying.'),
    ( 4, 'W-04', 'Austin Terrace, outside the bakery', 'Tall fellow, well over six foot. Broad. Dark jacket, the kind with the reflective bands on the sleeve. Carrying a long soft bag over his right shoulder.'),
    ( 5, 'W-05', 'Spadina Crescent, at the junction', 'I was waiting to cross. A man went past me towards the square side of the Loma House carrying something long. I assumed scaffolding.'),
    ( 6, 'W-06', 'Macpherson Mews, at the yard gates', 'There was a small grey car sitting in the lane with its engine running for a good twenty minutes. Nobody got out. It was facing the top of the lane.'),
    ( 7, 'W-07', 'Macpherson Mews, at the yard gates', 'Silver or grey, one of the little three door ones. When the bang went off it pulled away hard, up towards the top end.'),
    ( 8, 'W-08', 'Macpherson Mews, at the yard gates', 'I only caught the front of the plate. It began with a T. There was a seven in the numbers, near the back of them.'),
    ( 9, 'W-09', 'Wychwood Square, north railings', 'I looked up at the roofline afterwards because that is where the sound came from. I could not see anyone. The parapet is high.'),
    (10, 'W-10', 'Wychwood Square, west side', 'I heard one crack. Flat, not like a firework. My ears rang on the left side, so it came from my left.'),
    (11, 'W-11', 'Walmer Road, below the block', 'Nothing unusual until the crowd started running down towards us. I did not hear the shot from where I was.'),
    (12, 'W-12', 'Wychwood Square, by the bandstand', 'A woman near me said she saw movement on the roof opposite. I looked and saw nothing. She was very upset and I do not know her.'),
    (13, 'W-13', 'Austin Terrace, at the crossing', 'The man in the dark jacket had a lanyard or a card on his chest. He put it against the door and it opened for him.'),
    (14, 'W-14', 'Loma House, ground floor lobby', 'The lift was out that afternoon so anybody going up was using the stairs. I did not see who did.'),
    (15, 'W-15', 'Wychwood Square, refreshment stall', 'It was shoulder to shoulder. Eight hundred at least. You could not have picked one face out of it.'),
    (16, 'W-16', 'Macpherson Mews, upper end', 'A small light coloured hatchback came up the lane far too fast just after the bang and went straight across without stopping.'),
    (17, 'W-17', 'Wychwood Square, east side', 'I was filming the speech on my phone. The camera was pointed at the platform, not the buildings. There is nothing on it.'),
    (18, 'W-18', 'Austin Terrace, outside the chemist', 'Two men in work jackets went by during the afternoon, at different times. The second one was much taller than the first.'),
    (19, 'W-19', 'Wychwood Square, by the steps', 'The stewards had cleared a lane down the middle. Everything was normal right up until it was not.'),
    (20, 'W-20', 'Spadina Crescent, at the bus stop', 'I saw a man come down the side of the building at about a quarter past seven, walking, carrying the same long bag. He turned left at the bottom.'),
    (21, 'W-21', 'Wychwood Square, north-west corner', 'He went down where he stood. There was no scuffle, nobody near him. It came from a distance.'),
    (22, 'W-22', 'Macpherson Mews, at the yard gates', 'The car had somebody in the driver seat the whole time. I could not tell you anything about them. I was looking at the car.'),
    (23, 'W-23', 'Wychwood Square, near the platform', 'The wound was on his upper left side. He was turned slightly to his left, towards the north, when it happened.'),
    (24, 'W-24', 'Austin Terrace, outside the bakery', 'The tall one in the dark jacket used his right hand for the door card. He held the bag with his left.'),
    (25, 'W-25', 'Wychwood Square, south side', 'People were saying afterwards that it came from the roof of Loma House. I have no idea. I was facing the other way.'),
    (26, 'W-26', 'Walmer Road, at the corner', 'There was a delivery van up on Walmer Road most of the evening. I do not think it moved until well after nine.'),
    (27, 'W-27', 'Macpherson Mews, lower end', 'A grey hatchback, and a plate starting with T. I remember because my sister had a T plate. I did not get the numbers.'),
    (28, 'W-28', 'Wychwood Square, by the railings', 'One shot. Then a very long silence, then the screaming. It felt like ten seconds but it was probably two.'),
    (29, 'W-29', 'Wychwood Square, refreshment stall', 'I served hundreds of people. Nobody stood out. Nobody was watching the buildings.'),
    (30, 'W-30', 'Loma House, second floor', 'I was working late. I heard someone on the stairwell going up, quite slowly, some time after six. I did not look out.'),
    (31, 'W-31', 'Loma House, second floor', 'The roof door at the top of the stairs is supposed to be alarmed. It has not worked since the winter and everybody knows it.'),
    (32, 'W-32', 'Austin Terrace, at the crossing', 'The man in the jacket had a beard, or heavy stubble. Dark hair, going grey at the sides. Middle aged, I would say.'),
    (33, 'W-33', 'Wychwood Square, west side', 'A steward blew a whistle and told everyone to get down. By then it was already over.'),
    (34, 'W-34', 'Macpherson Mews, at the yard gates', 'It sat there long enough that I thought about asking it to move. Engine running the whole time. Grey, small, three doors.'),
    (35, 'W-35', 'Wychwood Square, north railings', 'There were pigeons off the Loma House roof about a minute before it happened. Something disturbed them.'),
    (36, 'W-36', 'Austin Terrace, outside the chemist', 'I could not put an age on him. Big, though. He filled the doorway.'),
    (37, 'W-37', 'Wychwood Square, by the bandstand', 'I have been to every one of these rallies for eleven years. Nothing like this. There was no trouble in the crowd at all.'),
    (38, 'W-38', 'Macpherson Mews, upper end', 'Whoever was driving knew the lane. They did not slow for the bend at the top.')
) AS t(n, witness_ref, location, statement)
